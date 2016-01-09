;;; config.lisp --- Functions for reading configuration files

;; Copyright (C) 2013-2016 Simen Heggestøyl

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
          (if content (string-trim +whitespace+ content) ""))))

(defun read-config-file (file-path)
  "Return a plist on the form (:SECTION \"content\" ...) from FILE-PATH."
  (string-list-to-plist
   (config-file-to-string-list file-path)))
