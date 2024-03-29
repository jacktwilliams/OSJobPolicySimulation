(defvar dir "/home/jack/dev/School/OSJobPolicySimulation/")
(defstruct transfer-params sender receiver block speed)
(defstruct holder name size allocated)
(defvar *tape* (make-holder :name 'tape :size most-positive-fixnum :allocated 0))
(defvar *tape->disk*)
(defvar *disk*)
(defvar *disk->memory*)
(defvar *memory*)
(defvar *memory-input*)
(defvar *disk->dma*)
(defvar *dma*)

(defstruct job name bursts inputs)
(defstruct input size)
(defvar *jobs* (list))
(defvar *job-size* 200)

(defstruct policy conditions actions)
(defstruct transfer transfer-params obj start-time)
(defstruct interrupt time result type)
(defstruct context storage-jobs-table transfers interrupts time)

(defparameter *context* nil
  "Shared state during execution.")

(defparameter *user-exec-time* 0
  "Amount of time CPU spends executing user jobs.")

(defun timestamped-print (time str &optional (out t))
  (format out "~a: ~a~%" time str))

(defun job-needs-input (job)
  "States whether job needs input in memory for next burst and is lacking that input currently."
  (let ((bursts (job-bursts job))
        (inputs (job-inputs job)))
    (and (null (job-in-location *memory-input*))
         (= (length bursts) (length inputs)))))

(defun get-input-size (job)
  (first (job-inputs job)))

(defun take-input-from-job (job)
  "Shorten JOB's input list and return the altered job."
  (let ((job (copy-job job))) ;; don't want the setf to change the job in the *jobs*
    (setf (job-inputs job) (cdr (job-inputs job)))
    job))

(defun take-burst-from-job (job)
  (let ((job (copy-job job)))
    (setf (job-bursts job) (cdr (job-bursts job)))
    job))

(defun get-transfer-params (loc1 loc2)
  (let ((n1 (holder-name loc1))
        (n2 (holder-name loc2)))
    (cond ((and (equal n1 'tape) (equal n2 'disk)) *tape->disk*)
          ((and (equal n1 'disk) (equal n2 'memory)) *disk->memory*)
          ((and (equal n1 'disk) (equal n2 'mem-in)) *disk->memory*)
          ((and (equal n1 'disk) (equal n2 'dma)) *disk->dma*))))

(defun get-time-for-transfer (params obj)
  (let* ((size (if (job-p obj) *job-size* (input-size obj)))
         (itrs (ceiling (/ size (transfer-params-block params)))))
    (* itrs (transfer-params-speed params))))

(defun remove-obj-from-location (loc &optional (obj nil))
  "Given LOCATION, if an OBJ is supplied, remove that object from location, otherwise remove the first object in location and return it."
  (let ((objs-in-location (cdr (assoc loc (context-storage-jobs-table *context*))))
        (removed-thing obj)
        (remove-test (lambda (key item)
                       (or (and (job-p key)
                                (job-p item)
                                (equal (job-name key) (job-name item)))
                           (and (input-p key)
                                (input-p item)
                                (equal (input-size key) (input-size item)))))))
    (if (not (null obj))
        (progn
          (setf removed-thing (find obj objs-in-location :test remove-test))
          (rplacd (assoc loc (context-storage-jobs-table *context*)) (remove removed-thing objs-in-location)))
        (progn
          (setf removed-thing (first objs-in-location))
          (rplacd (assoc loc (context-storage-jobs-table *context*)) (remove removed-thing objs-in-location))))
    (let ((obj-size (if (job-p removed-thing) *job-size* (if (input-p removed-thing) (input-size removed-thing)
                                                             0))))
      (setf (holder-allocated loc) (- (holder-allocated loc) obj-size)))
    removed-thing))

(defun add-obj-to-location (loc obj)
  (let* ((s-j-t (context-storage-jobs-table *context*))
         (obj-size (if (job-p obj) *job-size* (input-size obj)))
         (remaining-loc-space (- (holder-size loc) (holder-allocated loc)))
         (new-loc-allocated (+ (holder-allocated loc) obj-size))
         (loc-jobs (cdr (assoc loc s-j-t)))
         (new-loc-jobs (append loc-jobs (list obj))))
    (when (> obj-size remaining-loc-space) (error "Not enough space for object."))
    (setf (holder-allocated loc) new-loc-allocated)
    (rplacd (assoc loc s-j-t) new-loc-jobs)
    (setf (context-storage-jobs-table *context*) s-j-t)))

(defun job-burst-needs-input (job)
  (let ((inp-length (length (job-inputs job)))
        (burst-length (length (job-bursts job))))
    (= inp-length burst-length)))

(defun consume-burst (job)
  "Pop the first item off the JOB's burst list, knowing the job is in memory."
  (let ((real-job (remove-obj-from-location *memory* job))) ;; replace job with actual job in memory
    (when (and (not (null (job-in-location *memory*)))
               (job-p (job-in-location *memory*))) ;;sanity check. TODO: if this fixes bug, take note that no input should be in *memory* it should be in *memory-input*
      (error "Job should've been removed."))
    (when (= 0 (length (job-bursts real-job)))
      (error "No bursts left."))
    (let ((new-job (take-burst-from-job real-job)))
      (if (= 0 (length (job-bursts new-job)))
          (timestamped-print (context-time *context*) (concatenate 'string (job-name job) " finished."))
          (add-obj-to-location *memory* new-job))
      new-job)))

(defun consume-input (job)
  "Pop the first item off the JOB's input list list, knowing the job is in memory."
  (let ((real-job (remove-obj-from-location *memory* job))) ;; replace job with actual job in memory if it hasn't already been taken and sent as arg
    (when (not (null (job-in-location *memory*))) ;;sanity check
      (error "Job should've been removed."))
    (when (= 0 (length (job-inputs real-job)))
      (error "No inputs left."))
    (let ((inp (remove-obj-from-location *memory-input*)))
      (when (equal  nil inp)
        (error "No input was in memory.")))
    (let ((new-job (take-input-from-job real-job)))
      (add-obj-to-location *memory* new-job)
      new-job)))


(defun move-with-result (loc1 loc2 obj res)
  "Using CONTEXT, and locations LOC1 and LOC2, move OBJ from the first to the latter, running RES upon completion."
  (let* ((transfer-params (get-transfer-params loc1 loc2))
         (time (context-time *context*))
         (transfer (make-transfer :transfer-params transfer-params
                                  :start-time time
                                  :obj obj))
         (finish-time (+ time (get-time-for-transfer transfer-params obj)))
         (obj-name (if (job-p obj) (job-name obj) "input"))
         (result (lambda ()
                   (setf (context-time *context*) finish-time)
                   (timestamped-print (context-time *context*) (with-output-to-string (outs)
                                                                 (format outs "Transfer from ~a to ~a completed"
                                                                         (holder-name loc1) (holder-name loc2))))
                   (setf (context-transfers *context*) (remove transfer (context-transfers *context*)))
                   (remove-obj-from-location loc1 obj)
                   (add-obj-to-location loc2 obj)
                   (when (not (null res)) (funcall res)))))
    (timestamped-print time (with-output-to-string (outs) (format outs "Moving ~a from ~a to ~a" obj-name (holder-name loc1) (holder-name loc2))))
    (setf (context-transfers *context*) (append (context-transfers *context*) (list transfer)))
    (setf (context-interrupts *context*) (append (context-interrupts *context*) (list (make-interrupt :time finish-time :result result :type 'transfer))))))

(defun job-in-location (location)
  "Return the first job that exists in LOCATION."
  (let ((loc-jobs (cdr (assoc location (context-storage-jobs-table *context*)))))
    (first loc-jobs)))

(defun jobs-only-on-tape ()
  "Return T if no jobs exist outside of tape."
  (dolist (location (map 'list #'car (context-storage-jobs-table *context*)))
    (when (and (not (equal 'tape (holder-name location)))
               (job-p (job-in-location location)))
      (return-from jobs-only-on-tape nil)))
  (return-from jobs-only-on-tape t))

(defun job-is-running (job)
  (dolist (interrupt (context-interrupts *context*) nil)
    (when (equal (interrupt-type interrupt) (job-name job))
      (return-from job-is-running t))))

(defun put-inputs-on-tape (job)
  (dolist (input (job-inputs job))
    (add-obj-to-location *tape* (make-input :size input))))

(defun run-job ()
  "Knowing a job is in memory, execute it. This entails proper logging and removing the cpu burst from the job's exec-sequence."
  (let* ((job (job-in-location *memory*))
         (run-time (first (job-bursts job))))
    (when (job-needs-input job)
      (error "~a needs input" job))
    (timestamped-print (context-time *context*) (concatenate 'string "Start executing " (job-name job)))
    (setf *user-exec-time* (+ run-time *user-exec-time*))
    (setf (context-interrupts *context*) (append (context-interrupts *context*) (list (make-interrupt :time (+ run-time (context-time *context*))
                                                                                                      :type (job-name job)
                                                                                                      :result (lambda ()
                                                                                                                (let ((job (job-in-location *memory*)))
                                                                                                                  (timestamped-print (context-time *context*) (concatenate 'string "Done executing " (job-name job)))
                                                                                                                  (when (job-burst-needs-input job) (consume-input job))
                                                                                                                  (consume-burst job)))))))))

(defun next-interrupt ()
  "Find interrupt with next lowest time. Execute the interrupt's result and set the context time accordingly."
  (let ((interrupts (context-interrupts *context*))
        (time most-positive-fixnum)
        (next nil))
    (dolist (interrupt interrupts)
      (when (< (interrupt-time interrupt) time)
        (setf time (interrupt-time interrupt))
        (setf next interrupt)))
    (when (not (null next))
      (setf (context-time *context*) time)
      (when (not (null (interrupt-result next)))
        (funcall (interrupt-result next))))
    (return-from next-interrupt next)))

(defun every-policy-condition (policy)
  "Does every condition of the policy hold?"
  (let ((all t))
    (dolist (condition (policy-conditions policy))
      (when (not (funcall condition))
        (setf all nil)))
    (return-from every-policy-condition all)))

(defun run (jobs policies)
  "Run all JOBS according to POLICIES."
  (let* ((s-j-t (list (cons *tape* jobs) (cons *disk* nil) (cons *dma* nil) (cons *memory* nil) (cons *memory-input* nil))) ; all jobs start on tape
         (interrupts (list (make-interrupt :time 0 :result (lambda ())))) ; interrupt to trigger policy checks for initial actions
         (*context* (make-context :storage-jobs-table s-j-t :transfers nil :interrupts interrupts :time 0)))
    (do ((interrupt (next-interrupt) (next-interrupt)))
        ((null interrupt) t)
      (setf (context-interrupts *context*) (remove interrupt (context-interrupts *context*))) ; remove the interrupt now that it has been processed.
      (let ((policy-chosen nil))
        (dolist (policy policies)
          (when (and (not policy-chosen)
                   (every-policy-condition policy))
              (dolist (action (policy-actions policy)) (funcall action))
              (setf policy-chosen t)))))
    (let* ((total (context-time *context*))
           (system (- total *user-exec-time*)))
      (format t "Time to complete all jobs: ~a~%Time spent executing user jobs: ~a~%Time spent by the system: ~a~%" total *user-exec-time* system))))

(defun read-dat-file ()
  (defun read-jobs (in-file)
    "Given the IN-FILE, assume preliminary data has been read. Read job list."
    (do ((line (read-line in-file nil nil) (read-line in-file nil nil)))
        ((null line) t)
      (with-input-from-string (job line)
        (let ((name (symbol-name (read job)))
              (bursts (list))
              (inputs (list)))
          (do ((num (read job nil nil) (read job nil nil)))
              ((null num) t)
            (if (< (length inputs) (length bursts))
                (setf inputs (append inputs (list num)))
                (setf bursts (append bursts (list num)))))
          (setf *jobs* (append *jobs* (list (make-job :name name :inputs inputs :bursts bursts))))))))

  (with-open-file (in-file (concatenate 'string dir "test2.dat"))
    (setf *tape->disk* (make-transfer-params
                        :sender 'tape
                        :receiver 'disk
                        :block (read in-file)
                        :speed (read in-file)))
    (setf *disk* (make-holder :name 'disk :size (read in-file) :allocated 0))
    (setf *disk->memory* (make-transfer-params
                          :sender 'disk
                          :receiver 'memory
                          :block (read in-file)
                          :speed (read in-file)))
    (let ((total-mem (read in-file))
          (mem-in (read in-file)))
      (setf *memory* (make-holder :name 'memory :size total-mem :allocated 0))
      (setf *memory-input* (make-holder :name 'mem-in :size mem-in :allocated 0)))
    (setf *dma* (make-holder :name 'dma :size (read in-file) :allocated 0))
    (read-jobs in-file)))

(load (concatenate 'string dir "testing.lisp"))
(load (concatenate 'string dir "run-strategies.lisp"))
