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

(defstruct job name exec-sequence)
(defvar *jobs* (list))
(defvar *job-size* 200)
(defvar *inp-size* 100)

(defstruct policy conditions actions)
(defstruct transfer transfer-params obj start-time)
(defstruct interrupt time result)
(defstruct context storage-jobs-table transfers interrupts time)

(defun timestamped-print (time str &optional (out t))
  (format out "~a: ~a" time str))

(defun job-needs-input (job)
  (= 1 (mod (length (job-exec-sequence job))
            2)))

(defun get-transfer-params (loc1 loc2)
  (let ((n1 (holder-name loc1))
        (n2 (holder-name loc2)))
    (cond ((and (equal n1 'tape) (equal n2 'disk)) *tape->disk*)
          ((and (equal n1 'disk) (equal n2 'memory)) *disk->memory*)
          ((and (equal n1 'disk) (equal n2 'dma)) *disk->dma*))))

(defun get-time-for-transfer (params obj)
  (let* ((size (if (job-p obj) *job-size* *inp-size*))
        (itrs (ceiling (/ size (transfer-params-block params)))))
    (* itrs (transfer-params-speed params))))

(defun move-with-result (context loc1 loc2 obj res)
  "Using CONTEXT, and locations LOC1 and LOC2, move OBJ from the first to the latter, running RES upon completion."
  (let* ((transfer-params (get-transfer-params loc1 loc2))
         (time (context-time context))
         (transfer (make-transfer :transfer-params transfer-params
                                  :start-time time
                                  :obj obj))
         (finish-time (+ time (get-time-for-transfer transfer-params obj)))
         (result (lambda (context)
                   (setf (context-time context) finish-time)
                   (timestamped-print (context-time context) (with-output-to-string (outs)
                                                               (format outs "Transfer from ~a to ~a completed"
                                                                       (holder-name loc1) (holder-name loc2))))
                   (setf (context-transfers context) (remove transfer (context-transfers context)))
                   ;;remove job/input from loc1 and add to loc 2
                   (rplacd (assoc loc1 (context-storage-jobs-table context)) (remove obj (assoc loc1 (context-storage-jobs-table context))))
                   (rplacd (assoc loc2 (context-storage-jobs-table context)) (append (cdr (assoc loc2 (context-storage-jobs-table context))) (list obj)))
                   (funcall res context))))
    ;;todo: either print moving input or moving job-name
    (timestamped-print time (with-output-to-string (outs) (format outs "Moving thing from ~a to ~a" (holder-name loc1) (holder-name loc2))))
    (setf (context-transfers context) (append (context-transfers context) (list transfer)))
    (setf (context-interrupts context) (append (context-interrupts context) (list (make-interrupt :time finish-time :result result))))))

(defun job-in-location (context location)
  (let ((loc-jobs (cdr (assoc location (context-storage-jobs-table context)))))
    (= 1 (length loc-jobs))))

(defun jobs-only-on-tape (context)
  (dolist (location (map 'list #'car (context-storage-jobs-table context)))
    (when (and (not (equal 'tape (holder-name location)))
               (job-in-location context location))
      (return-from jobs-only-on-tape nil)))
  (return-from jobs-only-on-tape t))

#+dev(defun simple-run ()
  (let* ((p-move-job (make-policy :conditions (list jobs-only-on-tape)
                                  :actions (list (lambda (c) ))
	      #+dev(p-execute-job (make-policy :conditions (list (lambda (c) (let ((mem-jobs (cdr (assoc *memory* (context-storage-jobs-table c)))))
									  (and (job-p (first mem-jobs))
									       (job-needs-input (first mem-jobs))))))
					  :actions )))))))

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
         (context (make-context :storage-jobs-table s-j-t :transfers nil :interrupts interrupts :time 0)))
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

(defun int-jobs-only-on-tape-test (jobs)
  "Tests jobs-only-on-tape and consequently job-in-location"
  (let* ((s-j-t (list (cons *tape* jobs) (cons *disk* nil) (cons *dma* nil) (cons *memory* nil) (cons *memory-input* nil)))
         (s-j-t-f (list (cons *tape* jobs) (cons *disk* (list (first jobs))) (cons *dma* nil) (cons *memory* nil) (cons *memory-input* nil)))
         (context (make-context :storage-jobs-table s-j-t :transfers nil :interrupts nil))
         (context-f (make-context :storage-jobs-table s-j-t-f :transfers nil :interrupts nil)))
    (and (jobs-only-on-tape context)
         (not (jobs-only-on-tape context-f)))))

(defun test-get-transfer-params ()
  (equal *tape->disk* (get-transfer-params *tape* *disk*)))

(defun int-move-with-result-test (jobs)
  (let* ((s-j-t (list (cons *tape* jobs) (cons *disk* nil) (cons *dma* nil) (cons *memory* nil) (cons *memory-input* nil)))
         (context (make-context :storage-jobs-table s-j-t :transfers nil :interrupts nil :time 0))
         (flag nil))
    (move-with-result context *tape* *disk* (first jobs) (lambda (c) (setf flag t)))
    (next-interrupt context)
    (and (job-in-location context *disk*)
         (equal flag t))))
