#!/usr/bin/sbcl --script

;;; sparql-endpoint-analyzer.lisp --- SPARQL endpoint analyzer for PepeSearch

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

#-quicklisp
(let ((quicklisp-init-1
       (merge-pathnames "quicklisp/setup.lisp" (user-homedir-pathname)))
      (quicklisp-init-2
       "quicklisp/setup.lisp"))
  (or (when (probe-file quicklisp-init-1)
        (load quicklisp-init-1))
      (when (probe-file quicklisp-init-2)
        (load quicklisp-init-2))))

(let ((*standard-output* (make-broadcast-stream)))
  (ql:quickload :alexandria)
  (ql:quickload :cl-json)
  (ql:quickload :cl-ppcre)
  (ql:quickload :cl-who)
  (ql:quickload :drakma)
  (ql:quickload :parse-number)
  (ql:quickload :xmls)
  (ql:quickload :flexi-streams))

(load "util.lisp")
(load "config.lisp")

(defpackage :sparql-endpoint-analyzer
  (:use :cl :util :cl-ppcre :json :parse-number)
  (:import-from :config
                :read-config-file)
  (:import-from :alexandria
                :assoc-value :emptyp :hash-table-values :random-elt :when-let
                :when-let*))

(in-package :sparql-endpoint-analyzer)

;; -------------------------------------------------------- [ Configuration ]
(defvar *config* nil
  "Plist of global configurations, on the form (:SECTION VALUE).")

(defun conf (section)
  "Return configuration string linked to SECTION."
  (getf *config* section))

(defun init-prefix-map (prefixes)
  "Create a mapping from URIs to defined PREFIXES, and store it in *CONFIG*."
  (let ((split (split "\\s+" prefixes))
        (prefix-map (make-hash-table :test 'equal)))
    (loop for (_ prefix uri) on split by #'cdddr do
      (let ((prefix-stem (string-trim ":" prefix))
            (uri-stem (string-trim "<>" uri)))
        (setf (gethash uri-stem prefix-map) prefix-stem)))
    (setf (getf *config* :prefix-map) prefix-map)
    (setf (getf *config* :used-prefixes)
          (hash-table-values (conf :prefix-map)))))

(defun valid-prefix-p (prefix)
  "Return T if PREFIX can be used as a SPARQL prefix and as an ID in JS."
  (and (> (length prefix) 0)
       (every #'alpha-char-p prefix)
       (not (find prefix (conf :used-prefixes) :test #'equal))))

(defun fresh-random-prefix (prefixes)
  "Return a random string of 3 characters, not already found in PREFIXES."
  (flet ((rnd-chr () (random-elt "abcdefghijklmnopqrstuvwxyz")))
    (loop for candidate = (fmt "~a~a~a" (rnd-chr) (rnd-chr) (rnd-chr))
          until (not (find candidate prefixes :test #'equal))
          finally (return candidate))))

(defun guess-prefix (stem)
  "Return a sensible prefix for STEM."
  (or (let* ((split (reverse (rest (rest (split "/" stem)))))
             (candidates (mapcar (lambda (s) (take 3 s)) split)))
        (find-if #'valid-prefix-p candidates))
      (fresh-random-prefix (conf :used-prefixes))))

(defun uri-prefix (uri)
  "Return a prefix for URI. If a prefix doesn't already exist, create one,
and add it to the configuration."
  (let ((stem (uri-stem uri)))
    (or (gethash stem (conf :prefix-map))
        (let ((new-prefix (guess-prefix stem)))
          (setf (gethash stem (getf *config* :prefix-map)) new-prefix)
          (push new-prefix (getf *config* :used-prefixes))
          new-prefix))))

(defun init-config (file-path)
  "Initialize *CONFIG* based on config file found at FILE-PATH."
  (handler-case
      (setf *config* (read-config-file file-path))
    (sb-int:simple-file-error ()
      (fmt-err "[error] Configuration file '~a' not found.~%" file-path)
      (sb-ext:exit)))
  (init-prefix-map (conf :prefixes))
  (setf (getf *config* :exclusive-whitelist)
        (split #\Newline (conf :exclusive-whitelist)))
  (setf (getf *config* :whitelist)
        (split #\Newline (conf :whitelist)))
  (setf (getf *config* :blacklist)
        (split #\Newline (conf :blacklist)))
  *config*)

(defun exclusively-whitelisted-p (uri)
  "Return T if URI is listed under `exclusive-whitelist' in the current
configuration."
  (find-if (lambda (p) (string-prefix-p p uri)) (conf :exclusive-whitelist)))

(defun whitelisted-p (uri)
  "Return T if URI is listed under `whitelist' in the current configuration."
  (find-if (lambda (p) (string-prefix-p p uri)) (conf :whitelist)))

(defun blacklisted-p (uri)
  "Return T if URI is listed under `blacklist' in the current configuration."
  (find-if (lambda (p) (string-prefix-p p uri)) (conf :blacklist)))

(defun disregard-uri-p (uri)
  "Return T if URI should be disregarded. An URI is disregarded by any of the
following criteria:
 1) The exclusive whitelist feature is enabled, and the URI's stem is not
    found in the exclusive whitelist.
 2) The URI's stem is blacklisted, and not whitelisted.
 3) It doesn't look like a URI according to `looks-like-uri-p'.
 4) The URI is a stem without a resource (it ends in / or #)."
  (or (not (looks-like-uri-p uri))
      (string= (nth-value 1 (split-uri uri)) "")
      (if (conf :exclusive-whitelist)
          (not (exclusively-whitelisted-p uri))
          (and (blacklisted-p uri)
               (not (whitelisted-p uri))))))

(defun filter (uri-list)
  "Return a copy of URI-LIST filtered by `disregard-uri-p'."
  (remove-if #'disregard-uri-p uri-list :key #'first))

;; --------------------------------------------------------------- [ SPARQL ]
(define-condition sparql-transaction-time-out (error) ())

(define-condition unknown-content-type (error)
  ((content-type :initarg :content-type :reader content-type)
   (content :initarg :content :reader content)))

(define-condition unknown-response-format (error)
  ((response :initarg :response :reader response)))

(define-condition unauthorized (error)
  ((reason :initarg :reason :reader reason)))

(defun sparql-query (endpoint query)
  "Send QUERY to ENDPOINT; return result as string."
  (let ((url (fmt "~a?query=~a&output=~a"
                  endpoint (drakma:url-encode query :utf-8) "json")))
    (when (equal (conf :output-level) "debug")
      (fmt-err "~%[debug] Sending query:~%~a~%" query))
    (multiple-value-bind (res status headers uri stream must-close reason)
        (drakma:http-request
         url
         :preserve-uri t
         :accept (strcat "application/json,"
                         "application/sparql-results+json,"
                         "application/xml,"
                         "application/sparql-results+xml")
         :basic-authorization (let ((username (conf :username))
                                    (password (conf :password)))
                                (and username password
                                     (list username password))))
      (declare (ignore uri stream must-close))
      (when (search "timed out" res :test #'equal)
        (error 'sparql-transaction-time-out))
      (when (= status 401)
        (error 'unauthorized :reason reason))
      (values
       (if (stringp res)
           res
           (flexi-streams:octets-to-string res :external-format :utf-8))
       (assoc-value headers :content-type)))))

(defun xml-bindings-to-list (bindings)
  "Return a list the decoded XML BINDINGS."
  (mapcar
   (lambda (binding)
     (let ((name (car (cdaadr binding)))
           (type (car (caaddr binding)))
           (value (cadr (cdaddr binding))))
       (list (string-to-keyword name)
             (cons :type type)
             (cons :value value))))
   (cddr bindings)))

(defun parse-json-response (response)
  "Return a list of variable bindings from the JSON RESPONSE."
  (cond
    ;; Response format:
    ;; (:RESULTS ...
    ;;  (:BINDINGS
    ;;   ((:row1-col1 (:TYPE . "...") (:VALUE . "..."))
    ;;    (:row1-col2 (:TYPE . "...") (:VALUE . "..."))
    ;;    ...)
    ;;   ((:row2-col1 (:TYPE . "...") (:VALUE . "..."))
    ;;    (:row2-col2 (:TYPE . "...") (:VALUE . "..."))
    ;;    ...)
    ;;   ...))
    ((find :results response :key #'first)
     (let ((res (find :results response :key #'first)))
       (assoc-value (rest res) :bindings)))

    ;; Response format:
    ;; ((:COLUMNS "col1" "col2")
    ;;  (:ROWS
    ;;   (((:TYPE . "...") (:VALUE . "..."))
    ;;    ((:TYPE . "...") (:VALUE . "...")))
    ;;   (((:TYPE . "...") (:VALUE . "..."))
    ;;    ((:TYPE . "...") (:VALUE . "...")))))
    ((find :columns response :key #'first)
     (let ((cols (find :columns response :key #'first))
           (rows (find :rows response :key #'first)))
       (mapcar (lambda (r) (mapcar #'cons (rest cols) r)) (rest rows))))
    (t (error 'unknown-response-format :response response))))

(defun parse-sparql-response (raw-results content-type)
  "Parse raw response from the SPARQL server, and return the variable
bindings. Supported CONTENT-TYPES are JSON and XML.
Bindings are on the format:
  (((:col1 (:TYPE . \"...\") (:VALUE . \"...\"))
    (:col2 (:TYPE . \"...\") (:VALUE . \"...\"))
    ...)
   ((:col1 (:TYPE . \"...\") (:VALUE . \"...\"))
    (:col2 (:TYPE . \"...\") (:VALUE . \"...\"))
    ...)
   ...)"
  (cond
    ((search "json" content-type :test #'equal) ; JSON format
     (let ((parsed (with-input-from-string (s raw-results)
                     (json:decode-json s))))
       (parse-json-response parsed)))
    ((search "xml" content-type :test #'equal) ; XML format
     (let ((parsed (xmls:parse raw-results)))
       (mapcar
        #'xml-bindings-to-list
        (cddr (find "results" (cddr parsed) :key #'caar :test #'equal)))))
    (t (error 'unknown-content-type
              :content-type content-type
              :content raw-results))))

(defun query-to-bindings (endpoint query)
  "Return the list of bindings from sending QUERY to ENDPOINT."
  (multiple-value-call #'parse-sparql-response
    (sparql-query endpoint (strcat (conf :prefixes) query))))

(defun binding-to-uri (binding)
  "Extract the URI from a single binding."
  (assoc-value (rest binding) :value))

(defun bindings-to-lists (bindings)
  "Return a list of result based on BINDINGS."
  (mapcar (lambda (b) (mapcar #'binding-to-uri b))
          (remove-if (lambda (type) (equal "bnode" type))
                     bindings
                     :key #'cdadar)))

(defun query-to-uri-list (query)
  "Send QUERY to the current SPARQL endpoint, and return the results as a list
of URI lists."
  (bindings-to-lists (query-to-bindings (conf :endpoint) query)))

(defun split-uri (uri)
  "Split URI into stem and resource. The split is done at the last
  occurrence of `/', `#', or `:'."
  (when-let ((pos (position-if (lambda (c) (find c "/#:")) uri :from-end t)))
    (values (take (+ pos 1) uri) (subseq uri (+ pos 1) (length uri)))))

(defun uri-stem (uri)
  "Return the stem of URI."
  (nth-value 0 (split-uri uri)))

(defun uri-resource (uri)
  "Return the resource of URI."
  (nth-value 1 (split-uri uri)))

;; --------------------------------------------------- [ Query construction ]
(defun make-query
    (section mode &key (limit 0) (offset 0) concept property hard-limit)
  "Construct SPARQL of type SECTION.
MODE should be either `:quick' or `:paged', where the latter will retrieve
results with LIMIT and OFFSET.
Some of the sections require a CONCEPT to retrieve information about, and some
require further refinement through a PROPERTY.
HARD-LIMIT sets a limit for the query through the SPARQL LIMIT keyword."
  (ecase section
    (:concepts
     (fmt
      "SELECT DISTINCT ?concept WHERE { ?obj a ?concept . }~@[ LIMIT ~a~]"
      hard-limit))
    (:literals
     (fmt
      "SELECT ~a ?prop WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
       FILTER (isLiteral(?targetObj)) } ~a"
      (if (eq mode :paged) "REDUCED" "DISTINCT")
      concept
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:outgoing-links
     (fmt
      "SELECT ~a ?prop WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
         ?targetObj a ?targetType . } ~a"
      (if (eq mode :paged) "REDUCED" "DISTINCT")
      concept
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:target-types
     (fmt
      "SELECT DISTINCT ?targetType WHERE {
         ?obj a <~a> .
         ?obj <~a> ?targetObj .
         ?targetObj a ?targetType . } ~a"
      concept
      property
      (if hard-limit (fmt "LIMIT ~a" hard-limit) "")))
    (:incoming-links
     (fmt
      "SELECT ~a ?prop WHERE {
         ?obj a <~a> .
         ?sourceObj ?prop ?obj .
         ?sourceObj a ?sourceType . } ~a"
      (if (eq mode :paged) "REDUCED" "DISTINCT")
      concept
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:subclasses
     (fmt
      "SELECT ~a ?subclass WHERE {
         ?subclass rdfs:subClassOf <~a> .
         FILTER (?subclass != <~a>) .
         FILTER NOT EXISTS {
           ?subclass rdfs:subClassOf ?x .
           ?x rdfs:subClassOf <~a> .
           FILTER (?x != ?subclass && ?x != <~a>) } } ~a"
      (if (eq mode :paged) "REDUCED" "DISTINCT")
      concept concept concept concept
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:every-subclass
     (fmt
      "SELECT ~a ?subclass WHERE {
         ?subclass rdfs:subClassOf <~a> . } ~a"
      (if (eq mode :paged) "REDUCED" "DISTINCT")
      concept
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:literal-type
     (fmt
      "SELECT (DATATYPE(?targetObj) as ?type) WHERE {
         ?obj a <~a> .
         ?obj <~a> ?targetObj . } LIMIT 1"
      concept property))
    (:year-limits
     (fmt
      "SELECT (YEAR(MIN(?val)) AS ?minYear) (YEAR(MAX(?val)) AS ?maxYear)
       WHERE {
         ?X0 a <~a> .
         ?X0 <~a> ?val . }"
      concept property))
    (:numeric-limits
     (fmt
      "SELECT (MIN(?val) AS ?min) (MAX(?val) AS ?max) WHERE {
         ?X0 a <~a> .
         ?X0 <~a> ?val . }"
      concept property))
    (:property-values
     (fmt
      "SELECT ~a ?propVal WHERE {
         ?obj a <~a> .
         ?obj <~a> ?propVal . } ~a"
      (ecase mode (:quick "REDUCED") (:paged ""))
      concept property
      (if (eq mode :paged)
          (fmt "LIMIT ~a OFFSET ~a" limit offset)
          (if hard-limit (fmt "LIMIT ~a" hard-limit) ""))))
    (:deprecated-uris
     (fmt
      "SELECT DISTINCT ?uri WHERE {
         { ?uri a <http://www.w3.org/2002/07/owl#DeprecatedClass> . }
       UNION
         { ?uri a <http://www.w3.org/2002/07/owl#DeprecatedProperty> . }
       } ~@[ LIMIT ~a~]"
      hard-limit))
    (:label
     (fmt
      "SELECT ?label WHERE {
         <~a> <http://www.w3.org/2000/01/rdf-schema#label> ?label .
       } LIMIT 1"
      concept))))

;; ------------------------------------------------------------ [ Retrieval ]
(defun repeated-retrieve (section limit page results
                          &key concept property page-limit)
  (fmt-err "[page ~a] ~a~@[ for ~a~]~@[/~a~] ... "
           (+ page 1)
           (string-capitalize section)
           (and concept (uri-resource concept))
           (and property (uri-resource property)))
  (let ((offset (* limit page)))
    (handler-case
        (let* ((query
                (make-query
                 section :paged
                 :limit limit
                 :offset offset
                 :concept concept
                 :property property))
               (res (query-to-uri-list query))
               (n (length res)))
          (setf results (append res results))
          (fmt-err "ok (got ~a)~%" n)
          (if (or (/= n limit) (= (+ page 1) page-limit))
              (remove-duplicates results :test #'equal)
              (repeated-retrieve
               section limit (+ page 1) results
               :concept concept :property property :page-limit page-limit)))
      (sparql-transaction-time-out ()
        (fmt-err "timeout~%")
        (fmt-err "[status] Retrying in 20 seconds ...~%")
        (sleep 20)
        (repeated-retrieve
         section limit page results
         :concept concept :property property :page-limit page-limit)))))

(defun retrieve (section &optional concept property)
  (fmt-err "[get] ~a~@[ for ~a~]~@[/~a~] ... "
           (string-capitalize section)
           (and concept (uri-resource concept))
           (and property (uri-resource property)))
  (handler-case
      (let* ((query
              (make-query
               section
               :quick
               :offset 0
               :concept concept
               :property property
               :hard-limit (conf :hard-limit)))
             (res (query-to-uri-list query))
             (n (length res)))
        (fmt-err "ok (got ~a)~%" n)
        (remove-duplicates res :test #'equal :key #'first))
    (sparql-transaction-time-out ()
      (fmt-err "timeout~%")
      (fmt-err "[status] Going for paged retrieval ... ~%")
      (repeated-retrieve
       section (parse-integer (conf :results-per-page-limit)) 0 '()
       :concept concept
       :property property
       :page-limit (let ((page-limit (parse-integer (conf :page-limit))))
                     (and (/= page-limit 0) page-limit))))))

;; --------------------------------------------------------------- [ Labels ]
(defgeneric add-label (obj)
  (:documentation
   "Add a label to OBJ, if defined by rdfs:label in the dataset."))

(defun prettify-label (label)
  "Return a pretty LABEL, removing underscores and fixing CamelCase."
  (unless (emptyp label)
    (setq label (substitute #\Space #\_ label))

    (let ((split-label
           (split "(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])" label)))
      ;; Above regex works like the following:
      ;;  "lowercase"     => ("lowercase")
      ;;  "CamelCase"     => ("Camel" "Case")
      ;;  "CAPSThenCamel" => ("CAPS" "Then" "Camel")
      (fmt
       "~{~a~^ ~}"
       (cons
        (first split-label)
        (mapcar
         (lambda (w) (if (every #'upper-case-p w) w (string-downcase w)))
         (rest split-label)))))))

;; ------------------------------------------------------------- [ Concepts ]
(defstruct concept
  (uri "" :type string)
  (label "" :type string)
  (outgoing-links '() :type list)
  (incoming-links '() :type list)
  (subclasses '() :type list)
  (superclasses '() :type list)
  (display 'false)
  (primary 'true :type symbol)
  (literals '() :type list))

(defmethod add-label ((concept concept))
  (when-let ((label (caar (retrieve :label (concept-uri concept)))))
    (setf (concept-label concept) label)))

(defun get-concept (uri concept-list)
  "Return concept with URI from CONCEPT-LIST."
  (find uri concept-list :key #'concept-uri :test #'string=))

(defun guess-concept-display-property (concept)
  (dolist (l (concept-literals concept))
    (let ((type (uri-resource (literal-type l))))
      (when (equal type "string")
        (return-from guess-concept-display-property
          (literal-uri l)))))
  (when (concept-literals concept)
    (literal-uri (first (concept-literals concept)))))

(defun set-concept-display-properties (concept)
  (when-let ((guess (guess-concept-display-property concept)))
    (setf (concept-display concept) (resource-to-id guess))))

(defun sort-concept-fields (concept label-map)
  (flet ((uri-label (uri) (gethash uri label-map)))
    (setf (concept-outgoing-links concept)
          (sort (concept-outgoing-links concept) #'string<
                :key (lambda (l) (uri-label (link-target-uri l)))))
    (setf (concept-incoming-links concept)
          (sort (concept-incoming-links concept) #'string<
                :key (lambda (l) (uri-label (link-target-uri l)))))
    (setf (concept-subclasses concept)
          (sort (concept-subclasses concept) #'string<
                :key #'uri-label))
    (setf (concept-superclasses concept)
          (sort (concept-superclasses concept) #'string<
                :key #'uri-label))
    (setf (concept-literals concept)
          (sort (concept-literals concept) #'string<
                :key (lambda (l) (uri-label (literal-uri l)))))))

;; ---------------------------------------------------------------- [ Links ]
(defstruct link
  (uri "" :type string)
  (target-uri "" :type string))

(defun add-outgoing-links (concept)
  (let ((outgoing-links
         (filter (retrieve :outgoing-links (concept-uri concept)))))
    (setf (concept-outgoing-links concept)
          (mapcar (lambda (ol) (make-link :uri (first ol)))
                  outgoing-links))))

(defun add-link-types (concept)
  "Add target types for the outgoing links in CONCEPT.
Links with multiple possible target types will be duplicated once per
additional target type."
  (let ((outgoing-links (concept-outgoing-links concept)))
    ;; Throw away the links, we'll add them again below with target types
    ;; attached.
    (setf (concept-outgoing-links concept) nil)
    (dolist (link outgoing-links)
      (when-let ((target-uris
                  (retrieve
                   :target-types (concept-uri concept) (link-uri link))))
        (dolist (target-uri (mapcar #'first target-uris))
          (let ((new-link (make-link :uri (link-uri link)
                                     :target-uri target-uri)))
            (push new-link (concept-outgoing-links concept))))))))

(defun add-incoming-links (concept concept-list)
  (dolist (link (concept-outgoing-links concept))
    (when-let* ((target-uri (link-target-uri link))
                (target-concept (get-concept target-uri concept-list)))
      (push
       (make-link :uri (link-uri link) :target-uri (concept-uri concept))
       (concept-incoming-links target-concept)))))

;; ----------------------------------------------------------- [ Subclasses ]
(defun add-subclasses (concept concept-list)
  (let ((subclass-uris
         (mapcar #'first (retrieve :subclasses (concept-uri concept)))))
    (dolist (subclass-uri subclass-uris)
      (when-let ((subclass-concept (get-concept subclass-uri concept-list)))
        (setf (concept-primary subclass-concept) 'false)))
    (setf (concept-subclasses concept) subclass-uris)))

(defun remove-orphan-subclasses (concept concept-list)
  (setf (concept-subclasses concept)
        (remove-if-not
         (lambda (subclass)
           (get-concept subclass concept-list))
         (concept-subclasses concept))))

(defun add-superclasses (concept concept-list)
  (dolist (subclass (concept-subclasses concept))
    (push (concept-uri concept)
          (concept-superclasses (get-concept subclass concept-list)))))

;; ------------------------------------------------------------ [ XSD types ]
(defconstant +xsd-numeric-types+
  '("byte" "decimal" "double" "float" "int" "integer" "long"
    "negativeInteger" "nonNegativeInteger" "nonPositiveInteger"
    "positiveInteger" "short" "unsignedInt" "unsignedLong" "unsignedShort"))

(defconstant +xsd-year-types+
  '("date" "dateTime" "gYear" "gYearMonth"))

(defun xsd-numeric-p (type)
  (find type +xsd-numeric-types+ :test #'equal))

(defun xsd-year-p (type)
  (find type +xsd-year-types+ :test #'equal))

(defun xsd-year (date)
  "Return the year component of a xsd:date, xsd:dateTime, xsd:gYear, or
xsd:gYearMonth."
  (first (split "-" date)))

(defun xsd-ui-type (type)
  "Return an appropriate UI type for the given XSD data type."
  (cond
    ((xsd-numeric-p type) "range")
    ((xsd-year-p type) "datetimerange")
    (t "string")))

;; ------------------------------------------------------------- [ Literals ]
(defstruct literal
  (uri "" :type string)
  (type "http://www.w3.org/2001/XMLSchema#string" :type string)
  range-min
  range-max)

(defun add-literals (concept)
  (let ((literals (filter (retrieve :literals (concept-uri concept)))))
    (setf (concept-literals concept)
          (mapcar (lambda (l) (make-literal :uri (first l)))
                  literals))))

(defun add-literal-types (concept)
  (dolist (literal (concept-literals concept))
    (when-let ((type (first (retrieve :literal-type (concept-uri concept)
                                      (literal-uri literal)))))
      (setf (literal-type literal) (first type)))))

(defun add-literal-limits (concept)
  (dolist (literal (concept-literals concept))
    (when-let ((type (and (literal-type literal)
                          (uri-resource (literal-type literal)))))
      (when (or (xsd-numeric-p type)
                (xsd-year-p type))
        (when-let ((limits
                    (first
                     (retrieve
                      :numeric-limits
                      (concept-uri concept)
                      (literal-uri literal)))))
          (if (xsd-year-p type)
              (setf (literal-range-min literal) (xsd-year (first limits))
                    (literal-range-max literal) (xsd-year (second limits)))
              (setf (literal-range-min literal) (first limits)
                    (literal-range-max literal) (second limits))))))))

(defun remove-undefined-links (concept concept-list)
  "Remove links from CONCEPT with undefined target types.
Defined target types are determined by CONCEPT-LIST."
  (setf (concept-outgoing-links concept)
        (remove-if-not
         (lambda (target-uri)
           (get-concept target-uri concept-list))
         (concept-outgoing-links concept) :key #'link-target-uri))
  (setf (concept-incoming-links concept)
        (remove-if-not
         (lambda (target-uri)
           (get-concept target-uri concept-list))
         (concept-incoming-links concept) :key #'link-target-uri)))

;; --------------------------------------------------------------- [ Output ]
(defvar *deprecated-uris* nil
  "List of URIs that should be marked as deprecated in the JSON output.")

(defun to-json (obj)
  "Return JSON-representation of lisp OBJ."
  (cond
    ((looks-like-plist-p obj)
     (fmt "{~%~{~a: ~a~^,~%~}~%}" (mapcar #'to-json obj)))
    ((listp obj)
     (fmt "[~@[~%~{~a~^, ~}~%~]]" (mapcar #'to-json obj)))
    ((eq obj 'true)
     "true")
    ((eq obj 'false)
     "false")
    ((or (stringp obj) (keywordp obj) (symbolp obj))
     (fmt "\"~a\"" obj))
    (t obj)))

(defun resource-to-id (uri)
  (strcat
   (uri-prefix uri) "_"
   ;; Replace "dangerous" characters with underscore
   (substitute-if #\_ (lambda (char) (find char ".-")) (uri-resource uri))))

(defun uri-to-plist (uri)
  `(:|id|    ,(resource-to-id uri)
    :|uri|   ,uri
    ,@(when (find uri *deprecated-uris* :test #'string=)
         '(:|deprecated| true))))

(defun concepts-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (append
      (uri-to-plist (concept-uri c))
      (list
       :|label| (list :|en| (concept-label c))
       :|display| (concept-display c)
       :|primary| (concept-primary c))))))

(defstruct property
  (uri "" :type string)
  (label "" :type string))

(defmethod add-label ((property property))
  (when-let ((label (caar (retrieve :label (property-uri property)))))
    (setf (property-label property) label)))

(defun extract-datatype-properties (concepts)
  (let ((datatype-properties '()))
    (dolist (c concepts)
      (loop for l in (concept-literals c) do
        (pushnew (literal-uri l) datatype-properties :test #'string=)))
    (mapcar
     (lambda (uri)
       (make-property :uri uri :label (prettify-label (uri-resource uri))))
     datatype-properties)))

(defun extract-object-properties (concepts)
  (let ((object-properties '()))
    (dolist (c concepts)
      (loop for ol in (concept-outgoing-links c)
            do (pushnew (link-uri ol) object-properties :test #'string=))
      (loop for il in (concept-incoming-links c)
            do (pushnew (link-uri il) object-properties :test #'string=)))
    (mapcar
     (lambda (uri)
       (make-property :uri uri :label (prettify-label (uri-resource uri))))
     object-properties)))

(defun properties-to-json (properties)
  (to-json
   (loop for p in properties collect
     (append
      (uri-to-plist (property-uri p))
      `(:|label| (:|en| ,(property-label p)))))))

(defun outgoing-link-to-plist (link)
  (list :|propId| (resource-to-id (link-uri link))
        :|target| (resource-to-id (link-target-uri link))))

(defun incoming-link-to-plist (link)
  (list :|propId| (resource-to-id (link-uri link))
        :|source| (resource-to-id (link-target-uri link))))

(defun literal-to-plist (literal)
  (let* ((datatype-uri (literal-type literal))
         (datatype (and datatype-uri (uri-resource datatype-uri)))
         (range-min (literal-range-min literal))
         (range-max (literal-range-max literal))
         (numeric (or (xsd-numeric-p datatype)
                      (xsd-year-p datatype))))
    (append
     (list :|propId| (resource-to-id (literal-uri literal)))
     (list :|searchable| 'true)
     (list :|dataType| datatype)
     (list :|uiType| (xsd-ui-type datatype))
     (and
      numeric range-min range-max
      (handler-case
          (list
           :|min| (parse-number range-min)
           :|max| (parse-number range-max))
        (parse-error ()))))))

(defun outgoing-links-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (list
      :|typeId|
      (resource-to-id (concept-uri c))
      :|outgoingLinks|
      (mapcar #'outgoing-link-to-plist (concept-outgoing-links c))))))

(defun incoming-links-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (list
      :|typeId|
      (resource-to-id (concept-uri c))
      :|incomingLinks|
      (mapcar #'incoming-link-to-plist (concept-incoming-links c))))))

(defun subclasses-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (list
      :|typeId| (resource-to-id (concept-uri c))
      :|subclasses| (mapcar #'resource-to-id (concept-subclasses c))))))

(defun superclasses-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (list
      :|typeId| (resource-to-id (concept-uri c))
      :|superclasses| (mapcar #'resource-to-id (concept-superclasses c))))))

(defun literals-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (list
      :|typeId| (resource-to-id (concept-uri c))
      :|literalValues| (mapcar #'literal-to-plist (concept-literals c))))))

(defun print-json (collection category)
  (destructuring-bind (json-printing-function name)
      (ecase category
        (:concepts
         (list #'concepts-to-json "Types"))
        (:object-properties
         (list #'properties-to-json "ObjectProperties"))
        (:outgoing-links
         (list #'outgoing-links-to-json "OutgoingLinks"))
        (:incoming-links
         (list #'incoming-links-to-json "IncomingLinks"))
        (:subclasses
         (list #'subclasses-to-json "SubclassRelations"))
        (:superclasses
         (list #'superclasses-to-json "SuperclassRelations"))
        (:datatype-properties
         (list #'properties-to-json "DatatypeProperties"))
        (:literals
         (list #'literals-to-json "LiteralValues")))
    (when-let ((json (funcall json-printing-function collection)))
      (format t "var json~a = ~a;~%" name json))))

;; -------------------------------------------------------------- [ Slurper ]
(defun slurp (config-file-path)
  (init-config config-file-path)
  (handler-case
      (let ((concepts
             (mapcar
              (lambda (c)
                (let ((uri (first c)))
                  (make-concept
                   :uri uri
                   :label (prettify-label (uri-resource uri)))))
              (filter (retrieve :concepts)))))

        (dolist (c concepts)
          (add-outgoing-links c)
          (add-link-types c)
          (add-incoming-links c concepts)
          (add-subclasses c concepts)
          (add-literals c)
          (add-literal-types c)
          (add-literal-limits c)
          (add-label c))

        (dolist (c concepts)
          (remove-undefined-links c concepts)
          (set-concept-display-properties c)
          (remove-orphan-subclasses c concepts)
          (add-superclasses c concepts))

        (let ((*deprecated-uris* (mapcar #'first (retrieve :deprecated-uris)))
              (datatype-properties (extract-datatype-properties concepts))
              (object-properties (extract-object-properties concepts)))

          ;; Look for labels for the datatype and object properties
          (mapc #'add-label datatype-properties)
          (mapc #'add-label object-properties)

          ;; Sort concept-, datatype property- and object property lists
          (setf concepts (sort concepts #'string< :key #'concept-label))
          (setf datatype-properties
                (sort datatype-properties #'string< :key #'property-label))
          (setf object-properties
                (sort object-properties #'string< :key #'property-label))

          ;; Sort internal concept fields
          (let ((label-map (make-hash-table :test 'equal)))
            (dolist (c concepts)
              (setf (gethash (concept-uri c) label-map) (concept-label c)))
            (dolist (p datatype-properties)
              (setf (gethash (property-uri p) label-map) (property-label p)))
            (dolist (p object-properties)
              (setf (gethash (property-uri p) label-map) (property-label p)))
            (dolist (c concepts)
              (sort-concept-fields c label-map)))

          (print-json concepts :concepts)
          (print-json object-properties :object-properties)
          (print-json concepts :outgoing-links)
          (print-json concepts :incoming-links)
          (print-json concepts :subclasses)
          (print-json concepts :superclasses)
          (print-json datatype-properties :datatype-properties)
          (print-json concepts :literals)))

    (usocket:connection-refused-error ()
      (fmt-err "~%[error] Connection refused.~%"))
    (usocket:timeout-error ()
      (fmt-err "~%[error] Endpoint not responding.~%"))
    (unknown-content-type (err)
      (fmt-err
       "~%[error] Unknown content type from endpoint: ~a.~%~%Server response:~%~s"
       (content-type err) (content err)))
    (unknown-response-format (err)
      (fmt-err
       "~%[error] Unknown response format from endpoint.~%~%Server response format:~%~s"
       (response err)))
    (unauthorized (err)
      (fmt-err
       "~%[error] Unauthorized; lacking valid authentication credentials.
        Server response: ~s.
        Try setting [username] and [password] in the configuration file.~%"
       (reason err)))))

(defun main (&aux (args sb-ext:*posix-argv*))
  (if (find "compile" args :test #'string=)
      (sb-ext:save-lisp-and-die
       "sparql-endpoint-analyzer"
       :compression (if (find :sb-core-compression *features*) t nil)
       :toplevel #'main
       :executable t)
      (handler-case
          (slurp (or (second args) "settings.conf"))
        (sb-sys:interactive-interrupt ()
          (fmt-err "~%Bye.~%")))))

(main)
