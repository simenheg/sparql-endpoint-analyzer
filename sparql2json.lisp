;#!/usr/bin/sbcl --script

#-quicklisp
(let ((quicklisp-init-1
       (merge-pathnames "quicklisp/setup.lisp" (user-homedir-pathname)))
      (quicklisp-init-2
       "quicklisp/setup.lisp"))
  (or (when (probe-file quicklisp-init-1)
        (load quicklisp-init-1))
      (when (probe-file quicklisp-init-2)
        (load quicklisp-init-2))))

(let ((*standard-output* *error-output*))
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

(defpackage :sparql2json
  (:use :cl :util :alexandria :cl-ppcre :json :parse-number)
  (:import-from :config :read-config-file))

(in-package :sparql2json)

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
    (setf (getf *config* :prefix-map) prefix-map)))

(defun valid-prefix-p (prefix)
  "Return T if PREFIX can be used as a SPARQL prefix and as an ID in JS."
  (and (> (length prefix) 0)
       (every #'alpha-char-p prefix)
       (not (find prefix (hash-table-values (conf :prefix-map))
                  :test #'equal))))

(defun uri-guess-prefix (uri)
  "Return a sensible prefix for URI."
  (let ((stem (uri-stem uri)))
    (or (gethash stem (conf :prefix-map))
        (let* ((split (reverse (rest (rest (split "/" stem)))))
               (candidates (mapcar (lambda (s) (take 3 s)) split)))
          (find-if #'valid-prefix-p candidates))
        "unk")))

(defun init-config (file-path)
  "Initialize *CONFIG* based on config file found at FILE-PATH."
  (handler-case
      (setf *config* (read-config-file file-path))
    (sb-int:simple-file-error ()
      (fmt-err "ERROR: Configuration file '~a' not found.~%" file-path)
      (sb-ext:exit)))
  (init-prefix-map (conf :prefixes))
  (setf (getf *config* :uri-whitelist)
        (split #\Newline (conf :uri-whitelist)))
  (setf (getf *config* :uri-blacklist)
        (split #\Newline (conf :uri-blacklist)))
  *config*)

(defun whitelisted-p (uri)
  (find-if (lambda (p) (string-prefix-p p uri)) (conf :uri-whitelist)))

(defun blacklisted-p (uri)
  (find-if (lambda (p) (string-prefix-p p uri)) (conf :uri-blacklist)))

(defun disregard-uri-p (uri)
  (or (blacklisted-p uri)
      (and (conf :uri-whitelist)
           (not (whitelisted-p uri)))))

(defun filter (uri-list)
  (remove-if #'disregard-uri-p uri-list :key #'first))

;; --------------------------------------------------------------- [ SPARQL ]
(define-condition sparql-transaction-time-out (error) ())
(define-condition unknown-content-type (error)
  ((content-type :initarg :content-type :reader content-type)))

(defun sparql-query (endpoint query)
  "Send QUERY to ENDPOINT; return result as string."
  (let ((url (fmt "~a?query=~a&output=~a"
                  endpoint (drakma:url-encode query :utf-8) "json")))
    (multiple-value-bind (res status headers)
        (drakma:http-request
         url
         :preserve-uri t
         :accept "application/json, application/sparql-results+json, application/xml,application/sparql-results+xml")
      (declare (ignore status))
      (when (search "timed out" res :test #'equal)
        (error 'sparql-transaction-time-out))
      (values
       (if (stringp res)
           res
           (flexi-streams:octets-to-string res :external-format :utf-8))
       (assoc-value headers :content-type)))))

(defun xml-bindings-to-list (bindings)
  (mapcar
   (lambda (binding)
     (let ((name (car (cdaadr binding)))
           (type (car (caaddr binding)))
           (value (cadr (cdaddr binding))))
       (list (string-to-keyword name)
             (cons :type type)
             (cons :value value))))
   (cddr bindings)))

(defun parse-sparql-response (raw-results content-type)
  (cond
    ((search "json" content-type :test #'equal) ; JSON format
     (let ((parsed (with-input-from-string (s raw-results)
                     (json:decode-json s))))
       (assoc-value (rest (find :results parsed :key #'first)) :bindings)))
    ((search "xml" content-type :test #'equal) ; XML format
     (let ((parsed (xmls:parse raw-results)))
       (mapcar
        #'xml-bindings-to-list
        (cddr (find "results" (cddr parsed) :key #'caar :test #'equal)))))
    (t (error 'unknown-content-type :content-type content-type))))

(defun query-to-bindings (endpoint query)
  (multiple-value-call #'parse-sparql-response
    (sparql-query endpoint (strcat (conf :prefixes) query))))

(defun binding-to-uri (binding)
  (assoc-value (rest binding) :value))

(defun bindings-to-lists (bindings)
  (mapcar (lambda (b) (mapcar #'binding-to-uri b))
          (remove-if (lambda (type) (equal "bnode" type))
                     bindings
                     :key #'cdadar)))

(defun query-to-uri-list (query)
  (bindings-to-lists (query-to-bindings (conf :endpoint) query)))

(defun split-uri (uri)
  "Split URI into stem and resource. The split is done at the last
  occurrence of `/' or `#'."
  (when-let ((pos (position-if (lambda (c) (find c "/#")) uri :from-end t)))
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
  "MODE should be one of :quick or :paged."
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
    (:target-type
     (fmt
      "SELECT ?targetType WHERE {
         ?obj a <~a> .
         ?obj <~a> ?targetObj .
         ?targetObj a ?targetType . } LIMIT 1"
      concept property))
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
      hard-limit))))

;; ------------------------------------------------------------ [ Retrieval ]
(defun repeated-retrieve (section limit page results
                          &key concept property page-limit)
  (fmt-err "[page ~a] Get ~a~@[ for ~a~]~@[/~a~] ... "
           (+ page 1)
           (string-downcase section)
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
  (fmt-err "[quick] Get ~a~@[ for ~a~]~@[/~a~] ... "
           (string-downcase section)
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

;; ------------------------------------------------------------- [ Concepts ]
(defstruct concept
  (uri "" :type string)
  (outgoing-links '() :type list)
  (incoming-links '() :type list)
  (subclasses '() :type list)
  (superclasses '() :type list)
  (display 'false)
  (primary 'true :type symbol)
  (literals '() :type list))

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

(defun sort-concept-fields (concept)
  (setf (concept-outgoing-links concept)
        (sort (concept-outgoing-links concept) #'string< :key #'link-uri))
  (setf (concept-incoming-links concept)
        (sort (concept-incoming-links concept) #'string< :key #'link-uri))
  (setf (concept-subclasses concept)
        (sort (concept-subclasses concept) #'string<))
  (setf (concept-superclasses concept)
        (sort (concept-superclasses concept) #'string<))
  (setf (concept-literals concept)
        (sort (concept-literals concept) #'string< :key #'literal-uri)))

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
  (dolist (link (concept-outgoing-links concept))
    (when-let ((target-uri
                (retrieve
                 :target-type (concept-uri concept) (link-uri link))))
      (setf (link-target-uri link) (first (first target-uri))))))

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
    (when-let*
      ((type
        (and (literal-type literal)
             (uri-resource (literal-type literal))))
       (type-section-keyword
        (cond
          ((xsd-numeric-p type) :numeric-limits)
          ((xsd-year-p type) :year-limits)))
       (limits
        (first
         (retrieve
          type-section-keyword
          (concept-uri concept) (literal-uri literal)))))
      (setf (literal-range-min literal)
            (first limits))
      (setf (literal-range-max literal)
            (second limits)))))

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
  (strcat (uri-guess-prefix uri) "_" (uri-resource uri)))

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

(defun uri-to-plist (uri)
  `(:|id|    ,(resource-to-id uri)
    :|uri|   ,uri
    :|label| (:|en| ,(prettify-label (uri-resource uri)))
    ,@(when (find uri *deprecated-uris* :test #'string=)
         '(:|deprecated| true))))

(defun concepts-to-json (concepts)
  (to-json
   (loop for c in concepts collect
     (append
      (uri-to-plist (concept-uri c))
      (list
       :|display| (concept-display c)
       :|primary| (concept-primary c))))))

(defun extract-datatype-properties (concepts)
  (let ((datatype-properties '()))
    (dolist (c concepts)
      (loop for l in (concept-literals c) do
        (pushnew (literal-uri l) datatype-properties :test #'string=)))
    (sort datatype-properties #'string<)))

(defun datatype-properties-to-json (concepts)
  (to-json (mapcar #'uri-to-plist (extract-datatype-properties concepts))))

(defun extract-object-properties (concepts)
  (let ((object-properties '()))
    (dolist (c concepts)
      (loop for ol in (concept-outgoing-links c)
            do (pushnew (link-uri ol) object-properties :test #'string=))
      (loop for il in (concept-incoming-links c)
            do (pushnew (link-uri il) object-properties :test #'string=)))
    (sort object-properties #'string<)))

(defun object-properties-to-json (concepts)
  (to-json (mapcar #'uri-to-plist (extract-object-properties concepts))))

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
     (list :|uiType| (if numeric "range" "string"))
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

(defun print-json (concepts category)
  (destructuring-bind (json-printing-function name)
      (ecase category
        (:concepts
         (list #'concepts-to-json "Types"))
        (:object-properties
         (list #'object-properties-to-json "ObjectProperties"))
        (:outgoing-links
         (list #'outgoing-links-to-json "OutgoingLinks"))
        (:incoming-links
         (list #'incoming-links-to-json "IncomingLinks"))
        (:subclasses
         (list #'subclasses-to-json "SubclassRelations"))
        (:superclasses
         (list #'superclasses-to-json "SuperclassRelations"))
        (:datatype-properties
         (list #'datatype-properties-to-json "DatatypeProperties"))
        (:literals
         (list #'literals-to-json "LiteralValues")))
    (when-let ((json (funcall json-printing-function concepts)))
      (format t "var json~a = ~a;~%" name json))))

;; -------------------------------------------------------------- [ Slurper ]
(defun slurp (config-file-path)
  (init-config config-file-path)
  (handler-case
      (let ((concepts
             (mapcar (lambda (c) (make-concept :uri (first c)))
                     (filter (retrieve :concepts)))))
        (dolist (c concepts)
          (add-outgoing-links c)
          (add-link-types c)
          (add-incoming-links c concepts)
          (add-subclasses c concepts)
          (add-literals c)
          (add-literal-types c)
          (add-literal-limits c))

        (dolist (c concepts)
          (set-concept-display-properties c)
          (remove-orphan-subclasses c concepts)
          (add-superclasses c concepts))

        (setf concepts (sort concepts #'string< :key #'concept-uri))
        (dolist (c concepts)
          (sort-concept-fields c))

        (let ((*deprecated-uris*
               (mapcar #'first (retrieve :deprecated-uris))))
          (print-json concepts :concepts)
          (print-json concepts :object-properties)
          (print-json concepts :outgoing-links)
          (print-json concepts :incoming-links)
          (print-json concepts :subclasses)
          (print-json concepts :superclasses)
          (print-json concepts :datatype-properties)
          (print-json concepts :literals)))
    (usocket:timeout-error ()
      (fmt-err "~%Endpoint not responding.~%"))
    (unknown-content-type (err)
      (fmt-err "~%Unknown content type from endpoint: ~a.~%"
               (content-type err)))))

(defun main (&aux (args sb-ext:*posix-argv*))
  (if (find "compile" args :test #'string=)
      (sb-ext:save-lisp-and-die
       "sparql2json"
       :compression (if (find :sb-core-compression *features*) t nil)
       :toplevel #'main
       :executable t)
      (handler-case
          (slurp (or (second args) "settings.conf"))
        (sb-sys:interactive-interrupt ()
          (fmt-err "~%Bye.~%")))))

;; (main)
