(defun reset-state ()
  (defun remove-all (holder)
    (do ((obj (remove-obj-from-location holder) (remove-obj-from-location holder)))
        ((null obj) t)))
  (setf *user-exec-time* 0)
  (dolist (holder (list *disk* *dma* *memory* *memory-input*))
    (when (context-p *context*) ;; if context has been initialized clear everything off of each holder when resetting state
      (remove-all holder))
    (setf (holder-allocated holder) 0)))

(defun no-transfers-happening-cond ()
  (= 0 (length (context-transfers *context*))))

(defun job-needs-input-cond ()
  (let ((job (job-in-location *memory*)))
    (and (not (null job))
         (job-needs-input job)
         (no-transfers-happening-cond))))

(defun job-should-run-cond ()
  (let ((job (job-in-location *memory*)))
    (and (not (null job))
         (not (job-is-running job))
         (not (job-needs-input job)))))

(defun job-on-tape-cond ()
  (let ((job (job-in-location *tape*)))
    (and (not (null job))
         (job-p job))))

(defun move-job-from-tape-to-mem ()
  (let ((job (cadr (assoc *tape* (context-storage-jobs-table *context*)))))
    (move-with-result *tape* *disk* job
                      (lambda ()
                        (move-with-result *disk* *memory* job nil)))))

(defun move-input-from-tape-to-mem ()
  (let* ((job (job-in-location *memory*))
        (input (make-input :size (get-input-size job))))
    (move-with-result *tape* *disk* input
                      (lambda ()
                        (move-with-result *disk* *memory-input* input nil)))))

(defun move-input-from-tape-to-disk ()
  (let* ((job (job-in-location *memory*))
         (input (make-input :size (get-input-size job))))
    (move-with-result *tape* *disk* input nil)))

(defun move-input-from-disk-to-mem ()
  (let* ((job (job-in-location *memory*))
         (input (input-ready-at-loc *disk* job)))
    (when (null input)
      (error "input wasn't ready"))
    (move-with-result *disk* *memory-input* input nil)))

(defun add-interrupt-to-check-for-action ()
  "Add interrupts to check if there are additional things we can do concurrently."
  (setf (context-interrupts *context*) (append (context-interrupts *context*)
                                               (list (make-interrupt :time (context-time *context*) :result nil :type 'checker)))))

(defun simple-run ()
  (reset-state)
  (let* ((p-move-job (make-policy :conditions (list #'jobs-only-on-tape #'job-on-tape-cond)
                                  :actions (list #'move-job-from-tape-to-mem)))
         (p-job-needs-input (make-policy :conditions (list #'job-needs-input-cond)
                                         :actions (list #'move-input-from-tape-to-mem)))
         (p-run-job (make-policy :conditions (list #'job-should-run-cond)
                                 :actions (list #'run-job))))
    (run *jobs* (list p-move-job p-job-needs-input p-run-job))))

(defun ready-for-input-in-mem ()
  (ready-for-input-at-loc *memory-input*))

(defun ready-for-input-at-loc (loc)
  (let ((job (job-in-location *memory*))
        (remaining-inp-space (- (holder-size loc) (holder-allocated loc))))
    (and (not (null job))
         (<= (get-input-size job) remaining-inp-space)
         (no-transfers-happening-cond)
         (> (length (job-inputs job)) 0))))

(defun input-ready-at-loc (loc job)
  "Returns an input object if it's available, otherwise nil."
  (let* ((input (make-input :size (get-input-size job)))
         (loc-objs (cdr (assoc loc (context-storage-jobs-table *context*))))
         (avail-inp (find input loc-objs :test (lambda (key item)
                                                 (and (input-p item)
                                                      (equal (input-size key) (input-size item)))))))
    (if (not (null avail-inp))
        avail-inp
        nil)))

(defun buffered-run ()
  (reset-state)
  (let* ((p-move-job (make-policy :conditions (list #'jobs-only-on-tape #'job-on-tape-cond)
                                  :actions (list #'move-job-from-tape-to-mem)))
         (p-run-job (make-policy :conditions (list #'job-should-run-cond)
                                 :actions (list #'run-job #'add-interrupt-to-check-for-action))) ;;interrupt to check to see if we can start bringing in input
         (p-bring-input (make-policy :conditions (list #'ready-for-input-in-mem)
                                     :actions (list #'move-input-from-tape-to-mem #'add-interrupt-to-check-for-action)))) ;; interrupt to check to see if we can bring more input
    (run *jobs* (list p-move-job p-run-job p-bring-input))))


(defun buffered-spooled-run ()
  (reset-state)
  (let* ((p-move-job (make-policy :conditions (list #'jobs-only-on-tape #'job-on-tape-cond)
                                  :actions (list (lambda ()
                                                   (let ((job (job-in-location *tape*)))
                                                     (put-inputs-on-tape job)))
                                                 #'move-job-from-tape-to-mem)))
         (p-run-job (make-policy :conditions (list #'job-should-run-cond)
                                 :actions (list #'run-job #'add-interrupt-to-check-for-action))) ;;interrupt to check to see if we can start bringing in input
         (p-bring-input-to-disk (make-policy :conditions (list (lambda ()
                                                                 (let ((job (job-in-location *memory*)))
                                                                   (and (not (null job))
                                                                        (input-ready-at-loc *tape* job)
                                                                        (ready-for-input-at-loc *disk*)))))
                                             :actions (list #'move-input-from-tape-to-disk #'add-interrupt-to-check-for-action)))
         (p-bring-input-to-mem (make-policy :conditions (list (lambda ()
                                                                (let ((job (job-in-location *memory*)))
                                                                  (and (not (null job))
                                                                       (input-ready-at-loc *disk* job)
                                                                       (ready-for-input-at-loc *memory-input*)))))
                                            :actions (list #'move-input-from-disk-to-mem #'add-interrupt-to-check-for-action))) ;; interrupt to check to see if we can bring more input
         )
    (run *jobs* (list p-move-job p-run-job p-bring-input-to-mem p-bring-input-to-disk))))
