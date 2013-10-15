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

(defun looks-like-plist-p (list)
  "Return T if LIST looks like a plist (first element is a keyword)."
  (and (listp list) (keywordp (first list))))

(defun indented-list (list indent n)
  "Return a copy of LIST where INDENT spaces precedes every Nth element."
  (interleave
   (make-list (/ (length list) n) :initial-element (spaces indent)) list n))

(defun to-json (obj &optional (indent 0))
  (incf indent 4)
  (cond
    ((looks-like-plist-p obj)
     (with-output-to-string (s)
       (format
        s "{~%~{a~a\"~a\": \"~a\"~^,~%~}~%}"
        (mapcar (lambda (o) (to-json o indent))
                (indented-list obj indent 2))
        ;; s "{~%~{    \"~a\": \"~a\"~^,~%~}~%}"
        ;; (mapcar (lambda (o) (to-json o (+ indent 4))) obj)
        )))
    ((listp obj)
     (with-output-to-string (s)
       (format
        ;; s "[~%~{~a~^, ~}~%]"
        ;; (mapcar (lambda (o) (to-json o (+ indent 4))) obj)
        s "[~%~{b~a~a~^, ~}~%]"
        (mapcar (lambda (o) (to-json o indent))
                (indented-list obj indent 1)))))
    (t obj)))

;; (defun list-to-json (list)
;;   (with-output-to-string (s)
;;     (format s "[~%~{~a~^, ~}~%]" list)))

;; (defun plist-to-json (plist)
;;   (with-output-to-string (s)
;;     (format s "{~%~{    \"~a\": \"~a\"~^,~%~}~%}" plist)))

(defun resource-to-id (prefix resource)
  (strcat prefix "_" resource))

(defun uri-to-plist (concept-uri)
  (multiple-value-bind (stem resource) (split-uri concept-uri)
    (list :|id| (resource-to-id (uri-guess-prefix stem) resource)
          :|uri| concept-uri
          :|label| resource)))

(defun uri-list-to-json (uri-list)
  (let ((resources '()))
    (dolist (uri uri-list)
      (push (uri-to-plist uri) resources))
    (format t (to-json resources))))

(defun concepts-to-json (concepts)
  (format t "var json~aTypes = " (conf :dataset-name))
  (uri-list-to-json (mapcar #'concept-uri concepts)))

(defun datatype-properties-to-json (datatype-properties)
  (format t "var json~aDatatypeProperties = " (conf :dataset-name))
  (uri-list-to-json datatype-properties))

(defun object-properties-to-json (object-properties)
  (format t "var json~aObjectProperties = " (conf :dataset-name))
  (uri-list-to-json (mapcar #'second object-properties)))

(defun link-to-plist (link)
  (destructuring-bind (link-name target-type) link
    (list :|propId| link-name
          :|target| target-type)))

(defun outgoing-links-to-json (concepts)
  (format t "var json~aOutgoingLinks = " (conf :dataset-name))
  (let ((link-list
         (loop for c in concepts
               when (concept-outgoing-links c) collect
           (multiple-value-bind (stem resource) (split-uri (concept-uri c))
             (list :|typeId|
                   (resource-to-id (uri-guess-prefix stem) resource)
                   :|outgoingLinks|
                   (mapcar #'link-to-plist (concept-outgoing-links c)))))))
    (to-json link-list)))

;; ---------------------------------------------- [ SPARQL-entry inspectors ]
(defun sparql-entry-field (entry)
  (string (first entry)))

(defun sparql-entry-type (entry)
  (cdr (find :type (rest entry) :key #'car)))

(defun sparql-entry-value (entry)
  (cdr (find :value (rest entry) :key #'car)))

;; -------------------------------------------------------------- [ Slurper ]

;;; Hard limit: 10,000. Might not get all the results.

(defun make-query (section &optional concept)
  (ecase section
    (:concepts
     "SELECT DISTINCT ?concept WHERE { ?obj a ?concept . }")
    (:literals
     (fmt
      "SELECT REDUCED ?prop WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
       FILTER (isLiteral(?targetObj)) }"
      concept))
    (:outgoing-links
     (fmt
      "SELECT REDUCED ?prop ?targetType WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj . 
         ?targetObj a ?targetType . }"
      concept))
    (:incoming-links
     (fmt
      "SELECT REDUCED ?prop WHERE {
         ?obj a <~a> .
         ?something ?prop ?obj . }"
      concept))))

(defun retrieve (section &optional concept)
  (fmt-err "Retrieving ~a~@[ for ~a~] ... "
           (string-downcase section) (and concept (uri-resource concept)))
  (handler-case
      (let ((res (query-to-uri-list (make-query section concept))))
        (fmt-err "ok~%")
        ;; (fmt-err "~%~a~%~%" res)
        res)
    (sparql-transaction-time-out ()
      (fmt-err "timeout~%")
      (fmt-err "Retrying in 5 seconds ")
      (fmt-err "(meanwhile, tell Simen to optimize this query better) ...~%")
      (sleep 5)
      (retrieve section concept))))

;; types (concept flat list)
;; object properties (object properties flat list)
;; outgoing links (concept -> outgoing object property list)
;; incoming links (concept -> incoming object property list)
;; datatype properties (datatype property flat list)
;; literal values (concept -> literal value type/range list)

(defparameter test
  '(("http://data.computas.com/informasjonsmodell/organisasjon/Enhet"
     "http://data.computas.com/informasjonsmodell/organisasjon/nacekode"
     "http://data.computas.com/informasjonsmodell/nace/Nacekode")
    ("http://data.computas.com/informasjonsmodell/organisasjon/Enhet"
     "http://data.computas.com/informasjonsmodell/organisasjon/organisasjonsform"
     "http://data.computas.com/informasjonsmodell/organisasjon/Enhetskode")
    ("http://data.computas.com/informasjonsmodell/organisasjon/Enhet"
     "http://data.computas.com/informasjonsmodell/regnskapsregisteret/regnskap"
     "http://data.computas.com/informasjonsmodell/regnskapsregisteret/Regnskap")))

(defstruct concept uri outgoing-links incoming-links)

(defun slurp ()
  (init-config)
  (let ((concepts ;; (mapcar #'first (retrieve :concepts))
         )
        (datatype-properties '())
        (object-properties test))

    (push
     (make-concept
      :uri "http://data.computas.com/informasjonsmodell/organisasjon/Enhet")
     concepts)

    (push
     (make-concept
      :uri "http://data.computas.com/informasjonsmodell/organisasjon/Person")
     concepts)

    (push
     (make-concept
      :uri "http://data.computas.com/informasjonsmodell/lokasjon/Adresse")
     concepts)

    (loop for (concept-uri link-name target-type) in object-properties do
      (let ((concept (find concept-uri concepts
                           :key #'concept-uri
                           :test #'string=)))
        (pushnew (list link-name target-type)
                 (concept-outgoing-links concept)
                 :test #'equal)))

    (concepts-to-json concepts)
    ;; (object-properties-to-json object-properties)
    ;; (outgoing-links-to-json concepts)
    
    ;; (dolist (c concepts)
    ;;   (dolist (dprop (retrieve :literals c))
    ;;     (pushnew (first dprop) datatype-properties :test #'string=)))

    ;; (dolist (c concepts)
    ;;   (dolist (oprop (retrieve :outgoing-links c))
    ;;     (pushnew (first oprop) object-properties :test #'string=)))

    ;; (dolist (c concepts)
    ;;   (dolist (oprop (retrieve :incoming-links c))
    ;;     (pushnew (first oprop) object-properties :test #'string=)))
))
