;;; util.lisp --- Utility definitions

;; Copyright (C) 2013-2015 Simen Heggestøyl

;; This program is free software: you can redistribute it and/or modify it
;; under the terms of the GNU General Public License as published by the Free
;; Software Foundation, either version 3 of the License, or (at your option)
;; any later version.

;; This program is distributed in the hope that it will be useful, but WITHOUT
;; ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
;; FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
;; more details.

;; You should have received a copy of the GNU General Public License along
;; with this program.  If not, see <http://www.gnu.org/licenses/>.

(defpackage :util
  (:use :cl)
  (:export
   :+whitespace+
   :fmt
   :fmt-err
   :looks-like-plist-p
   :looks-like-uri-p
   :spaces
   :strcat
   :string-prefix-p
   :string-to-keyword
   :take))

(in-package :util)

(defconstant +whitespace+ '(#\Space #\Tab #\Newline))

(defun fmt (&rest args)
  "Return format string ARGS."
  (apply #'format nil args))

(defun fmt-err (&rest args)
  "Print format string ARGS to standard error output."
  (apply #'format *error-output* args))

(defun looks-like-plist-p (list)
  "Return T if LIST looks like a plist (first element is a keyword)."
  (and (listp list) (keywordp (first list))))

(defun looks-like-uri-p (string)
  "Return T if STRING looks like an URI."
  (and (>= (length string) 4)
       (string= (subseq string 0 4) "http")))

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
