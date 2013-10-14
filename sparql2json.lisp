(ql:quickload :drakma)
(ql:quickload :cl-json)
(ql:quickload :cl-who)
(ql:quickload :hunchentoot)
(ql:quickload :alexandria)
(ql:quickload :cl-ppcre)

(load "util.lisp")
(load "config.lisp")

(defpackage :sparql2json
  (:use :cl :util :alexandria :cl-ppcre :json)
  (:import-from :config :read-config-file))

(in-package :sparql2json)

;; -------------------------------------------------------- [ Configuration ]
(defparameter *config* nil)

(defun conf (section)
  "Return configuration string linked to SECTION."
  (getf *config* section))

(defun init-prefix-map (prefixes)
  (let ((split (split :whitespace-char-class prefixes))
        (prefix-map (make-hash-table :test 'equal)))
    (loop for (_ prefix uri) on split by #'cdddr do
      (let ((prefix-stem (string-trim ":" prefix))
            (uri-stem (string-trim "<>" uri)))
        (setf (gethash uri-stem prefix-map) prefix-stem)))
    (setf (getf *config* :prefix-map) prefix-map)))

(defun uri-guess-prefix (uri)
  (or (gethash uri (conf :prefix-map))
      (let ((guess (first (last (split "/" uri)))))
        (take 3 guess))))

(defun init-config (&optional (file-path "settings.conf"))
  "Initialize *CONFIG* based on config file found at FILE-PATH."
  (setf *config* (read-config-file file-path))
  (init-prefix-map (conf :prefixes)))

(defun filter-whitelist (values)
  (loop for prefix in (split #\Newline (conf :uri-whitelist)) do
    (setf values
          (remove-if-not
           (lambda (v) (string-prefix-p prefix v)) values :key #'first)))
  values)

(defun filter-blacklist (values)
  (loop for prefix in (split #\Newline (conf :uri-blacklist)) do
    (setf values
          (remove-if
           (lambda (v) (string-prefix-p prefix v)) values :key #'first)))
  values)

;; --------------------------------------------------------------- [ SPARQL ]
(define-condition sparql-transaction-time-out (error) ())

(defun sparql-query (endpoint query)
  "Send QUERY to ENDPOINT; return result as string."
  (let* ((mode (if (search "CONSTRUCT" query) 'CONSTRUCT 'SELECT))
         (url (format nil "~a?query=~a&output=~a"
                      endpoint
                      (hunchentoot:url-encode query)
                      (if (eq mode 'CONSTRUCT) "ttl" "json")))
         (res (drakma:http-request url :preserve-uri t)))
    (when (search "timed out" res)
      (error 'sparql-transaction-time-out))
    (if (stringp res)
        (list 'text res)
        (list 'json (flexi-streams:octets-to-string
                     res
                     :external-format :utf-8)))))

(defun query-to-bindings (endpoint query)
  (destructuring-bind (format data)
      (sparql-query endpoint (strcat (conf :prefixes) query))
    (if (eq format 'text)
        (cl-who:escape-string data)
        (let* ((json-results
                (with-input-from-string (s data) (json:decode-json s)))
               (results (find :results json-results :key #'first))
               (bindings (assoc :bindings (rest results))))
          (rest bindings)))))

(defun bindings-to-lists (bindings)
  (mapcar (lambda (b) (mapcar #'cdaddr b)) bindings))

(defun query-to-uri-list (query)
  (let ((values
         (bindings-to-lists
          (query-to-bindings (conf :endpoint) query))))
    (remove-duplicates
     (filter-blacklist
      (filter-whitelist values))
     :test #'string=
     :key #'first)))

(defun split-uri (uri)
  "Split URI into stem and resource."
  (let ((pos (+ (position #\/ uri :from-end t) 1)))
    (values (take pos uri) (subseq uri pos (length uri)))))

(defun uri-resource (uri)
  "Return the resource from a URI."
  (nth-value 1 (split-uri uri)))

(defun list-to-json (list)
  (with-output-to-string (s)
    (format s "[~%~{~a~^, ~}~%]" list)))

(defun plist-to-json (plist)
  (with-output-to-string (s)
    (format s "{~%~{    \"~a\": \"~a\"~^,~%~}~%}" plist)))

(defun resource-to-id (prefix resource)
  (strcat prefix "_" resource))

(defun uri-list-to-json (uri-list)
  (let ((resources '()))
    (dolist (uri uri-list)
      (multiple-value-bind (stem resource) (split-uri uri)
        (push
         (list "id" (resource-to-id (uri-guess-prefix stem) resource)
               "uri" uri
               "label" resource
               "display" "rdf_label")
         resources)))
    (format t (list-to-json (mapcar #'plist-to-json resources)))))

;; ---------------------------------------------- [ SPARQL-entry inspectors ]
(defun sparql-entry-field (entry)
  (string (first entry)))

(defun sparql-entry-type (entry)
  (cdr (find :type (rest entry) :key #'car)))

(defun sparql-entry-value (entry)
  (cdr (find :value (rest entry) :key #'car)))

;; -------------------------------------------------------------- [ Slurper ]

;;; TODO:THIS IS TIMING OUT
;;;
;;; (time (retrieve :literals "http://data.computas.com/informasjonsmodell/regnskapsregisteret/Regnskap"))

(defun make-query (section &optional concept)
  (ecase section
    (:concepts
     "SELECT DISTINCT ?concept WHERE { ?obj a ?concept . }")
    (:literals
     (fmt
      "SELECT ?prop WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
       FILTER (isLiteral(?targetObj)) }"
      concept))
    (:outgoing-links
     (fmt
      "SELECT ?prop ?targetType WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj . 
         ?targetObj a ?targetType . }"
      concept))
    (:incoming-links
     (fmt
      "SELECT ?prop WHERE {
         ?obj a <~a> .
         ?something ?prop ?obj . }"
      concept))
    (:heavy
     "SELECT DISTINCT ?link WHERE { ?enhet a ?anything . ?enhet ?link ?obj . ?obj a ?what . }")))

(defun retrieve (section &optional concept)
  (fmt-err "Retrieving ~a~@[ for ~a~] ... "
           (string-downcase section) (and concept (uri-resource concept)))
  (handler-case
      (let ((res (query-to-uri-list (make-query section concept))))
        (fmt-err "ok~%")
        (fmt-err "~%~a~%~%" res)
        res)
    (sparql-transaction-time-out ()
      (fmt-err "timeout~%")
      (fmt-err "Retrying in 5 seconds ")
      (fmt-err "(meanwhile, tell Simen to optimize this query better) ...~%")
      (sleep 5)
      (retrieve section concept))))

(defun slurp ()
  (init-config)
  (let ((concepts (mapcar #'first (retrieve :concepts))))
    (dolist (c concepts)
      (retrieve :literals c))
    (dolist (c concepts)
      (retrieve :outgoing-links c))
    (dolist (c concepts)
      (retrieve :incoming-links c))))
