(defpackage :config
  (:use :cl :util :alexandria :cl-ppcre)
  (:export :read-config-file))

(in-package :config)

(defun config-file-to-string-list (file-path)
  (let* ((file-contents (read-file-into-string file-path))
         (without-comments (regex-replace-all "#+ +.*" file-contents "")))
    (rest (split "\\[(.*)\\]" without-comments :with-registers-p t))))

(defun string-list-to-plist (string-list)
  (loop for (section content) on string-list by #'cddr append
    (list (string-to-keyword section)
          (string-trim *whitespace* content))))

(defun read-config-file (file-path)
  "Return a plist on the form (:SECTION \"content\" ...) from FILE-PATH."
  (string-list-to-plist
   (config-file-to-string-list file-path)))
