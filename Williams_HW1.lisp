(defvar dir "/home/jack/Storage/Nextcloud/School/OperatingSystems/")
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

(defstruct job name exec-sequence)
(defvar *jobs* (list))

(defstruct policy conditions actions)
(defstruct transfer transfer-params start-time completion-time)
(defstruct interrupt time result)
(defstruct context storage-jobs-table transfers interrupts)

(defun job-needs-input (job)
  (= 1 (mod (length (job-exec-sequence job))
            2)))

(defun simple-run ()
  (let* ((p-execute-job (make-policy :conditions (list (lambda (c) (let ((mem-jobs (cdr (assoc *memory* (context-storage-jobs-table c)))))
                                                                     (and (job-p (first mem-jobs))
                                                                          (job-needs-input (first mem-jobs))))))
                                     :actions ))) ;policy-ex-job: if a job is in memory
         )))

(defun next-interrupt (context)
  "Find interrupt with next lowest time from INTERRUPTS and execute its result on CONTEXT."
  (let ((interrupts (context-interrupts context)) (time most-positive-fixnum) (next nil))
    (dolist (interrupt interrupts)
      (when (< (interrupt-time interrupt) time)
        (setf time (interrupt-time interrupt))
        (setf next interrupt)))
    (when (not (null next))
      (funcall (interrupt-result next) context))
    (return-from next-interrupt next)))


(defun every-policy-condition (policy context)
  (let ((all t))
    (dolist (condition (policy-conditions policy))
      (when (not (funcall condition context))
        (setf all nil)))
    (return-from every-policy-condition all)))

(defun run (jobs policies)
  (let* ((s-j-t (list (cons *tape* jobs) (cons *disk* nil) (cons *dma* nil) (cons *memory* nil) (cons *memory-input* nil))) ; all jobs start on tape
         (interrupts (list (make-interrupt :time 0 :result (lambda (c) )))) ; interrupt to trigger policy checks for initial actions
         (context (make-context :storage-jobs-table s-j-t :transfers nil :interrupts interrupts)))
    (do ((interrupt (next-interrupt context) (next-interrupt context)))
        ((null interrupt) t)
      (setf (context-interrupts context) (remove interrupt (context-interrupts context))) ; remove the interrupt now that it has been processed.
      (dolist (policy policies)
        (if (every-policy-condition policy context)
            (dolist (action (policy-actions policy)) (funcall action context)))))))

(defun read-dat-file ()
  (defun read-jobs (in-file)
    "Given the IN-FILE, assume preliminary data has been read. Read job list."
    (do ((line (read-line in-file nil nil) (read-line in-file nil nil)))
        ((null line) t)
      (with-input-from-string (job line)
        (let ((name (read job))
              (exec-sequence '()))
          (do ((num (read job nil nil) (read job nil nil)))
              ((null num) t)
            (setf exec-sequence (append exec-sequence (list num))))
          (setf *jobs* (append *jobs* (list (make-job :name name :exec-sequence exec-sequence))))))))

  (with-open-file (in-file (concatenate 'string dir "jobs.dat"))
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
      (setf *memory* (make-holder :name 'memory :size (- total-mem mem-in) :allocated 0))
      (setf *memory-input* (make-holder :name 'mem-in :size mem-in :allocated 0)))
    (setf *dma* (make-holder :name 'dma :size (read in-file) :allocated 0))
    (read-jobs in-file)))


(defun test-next-interrupt ()
  (let* ((interrupts (list (make-interrupt :time 10 :result (lambda (c) (print "hey")))
                           (make-interrupt :time 20 :result (lambda (c) (print "hey")))))
         (context (make-context :storage-jobs-table nil :transfers nil :interrupts interrupts))
         (res-time (interrupt-time (next-interrupt context)))
         (context1 (make-context :interrupts (list)))
         (res1 (next-interrupt context1))
         (res (and (= res-time 10) (null res1))))
    (print res-time)
    (print res)
    res))

(defun int-run-test ()
  "Tests evaluation of policies. Tests the integration with next-interrupt by having actions that sets more interrupts."
  (let* ((flag nil)
         (next-interrupt-set nil)
         (pol1 (make-policy :conditions (list (lambda (c) t)) :actions (list (lambda (c) (print "action"))
                                                                             (lambda (c) (if (equal t c) flag (setf flag t)))
                                                                             (lambda (c) (when (null next-interrupt-set)
                                                                                           (setf (context-interrupts c) (list (make-interrupt :time 10 :result (lambda (c) (print "interrupt processed")))))
                                                                                           (setf next-interrupt-set t)))))))
    (run nil (list pol1))
    (equal (funcall (second (policy-actions pol1)) t) t)))
