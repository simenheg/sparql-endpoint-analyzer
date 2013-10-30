(defpackage :util
  (:use :cl)
  (:export
   :+utf-8+
   :*whitespace*
   :fmt
   :fmt-err
   :spaces
   :strcat
   :string-prefix-p
   :string-to-keyword
   :take))

(in-package :util)

(defconstant +utf-8+
  (flexi-streams:make-external-format :utf8 :eol-style :lf))

(defvar *whitespace* '(#\Space #\Tab #\Newline))

(defun fmt (&rest args)
  "Return format string ARGS."
  (apply #'format nil args))

(defun fmt-err (&rest args)
  "Print format string ARGS to standard error output."
  (apply #'format *error-output* args))

(defun spaces (amount)
  "Return a string of AMOUNT spaces."
  (make-string amount :initial-element #\Space))

(defun split-seq (n seq)
  "Return the two sequences resulting from splitting SEQ at position N."
  (if (> n (length seq))
      (values seq '())
      (values (subseq seq 0 n)
              (subseq seq n (length seq)))))

(defun strcat (&rest strings)
  "Return the concatenation of STRINGS."
  (apply #'concatenate 'string strings))

(defun string-prefix-p (prefix string)
  "Return T if PREFIX is a prefix of STRING, else return NIL."
  (let ((l1 (length prefix))
        (l2 (length string)))
    (and (<= l1 l2)
         (string= prefix (subseq string 0 l1)))))

(defun string-to-keyword (string)
  "Convert STRING to a Lisp keyword."
  (intern (string-upcase string) "KEYWORD"))

(defun take (n seq)
  "Return a sequence of (at most) the N first elements of SEQ."
  (if (> n (length seq))
      seq
      (subseq seq 0 n)))
