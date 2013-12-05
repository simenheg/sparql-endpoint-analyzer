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
  (ql:quickload :flexi-streams)
  (ql:quickload :drakma)
  (ql:quickload :cl-json)
  (ql:quickload :cl-who)
  (ql:quickload :alexandria)
  (ql:quickload :cl-ppcre))

(load "util.lisp")
(load "config.lisp")

(defpackage :sparql2json
  (:use :cl :util :alexandria :cl-ppcre :json)
  (:import-from :config :read-config-file))

(in-package :sparql2json)

;; -------------------------------------------------------- [ Configuration ]
(defparameter *config* nil
  "Plist of global configurations, on the form (:SECTION VALUE).")

(defun conf (section)
  "Return configuration string linked to SECTION."
  (getf *config* section))

(defun init-prefix-map (prefixes)
  "Create a mapping from URIs to defined PREFIXES, and store it in *CONFIG*."
  (let ((split (split :whitespace-char-class prefixes))
        (prefix-map (make-hash-table :test 'equal)))
    (loop for (_ prefix uri) on split by #'cdddr do
      (let ((prefix-stem (string-trim ":" prefix))
            (uri-stem (string-trim "<>" uri)))
        (setf (gethash uri-stem prefix-map) prefix-stem)))
    (setf (getf *config* :prefix-map) prefix-map)))

(defun uri-guess-prefix (uri)
  "Return a sensible prefix for URI."
  (or (gethash uri (conf :prefix-map))
      (let ((guess (first (last (split "/" uri)))))
        (take 3 guess))))

(defun init-config (file-path)
  "Initialize *CONFIG* based on config file found at FILE-PATH."
  (handler-case
      (setf *config* (read-config-file file-path))
    (sb-int:simple-file-error ()
      (fmt-err "ERROR: Configuration file '~a' not found.~%" file-path)
      (sb-ext:exit)))
  (init-prefix-map (conf :prefixes)))

(defun uri-p (string)
  "Return T if STRING looks like an URI."
  (and (>= (length string) 4)
       (string= (subseq string 0 4) "http")))

(defun filter-whitelist (values)
  "Return copy of VALUES where every value has a prefix from the whitelist."
  (when-let ((prefixes (split #\Newline (conf :uri-whitelist))))
    (setf values
          (remove-if-not
           (lambda (v) (or (not (uri-p v))
                      (some (lambda (p) (string-prefix-p p v)) prefixes)))
           values :key #'first)))
  values)

(defun filter-blacklist (values)
  "Return copy of VALUES where no value has a prefix from the blacklist."
  (loop for prefix in (split #\Newline (conf :uri-blacklist)) do
    (setf values
          (remove-if
           (lambda (v) (string-prefix-p prefix v)) values :key #'first)))
  values)

;; --------------------------------------------------------------- [ SPARQL ]
(define-condition sparql-transaction-time-out (error) ())

(defstruct concept
  (uri "" :type string)
  (outgoing-links '() :type list)
  (incoming-links '() :type list)
  (literals '() :type list))

(defun sparql-query (endpoint query)
  "Send QUERY to ENDPOINT; return result as string."
  (let* ((mode (if (search "CONSTRUCT" query) 'CONSTRUCT 'SELECT))
         (url (format nil "~a?query=~a&output=~a"
                      endpoint
                      (drakma:url-encode query :utf-8)
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

(defun binding-to-uri (binding)
  (let ((type (cdadr binding)))
    (cond 
      ((string= type "uri")
       (cdaddr binding))
      ((string= type "typed-literal")
       (cdr (cadddr binding)))
      ((string= type "literal")
       (cdaddr binding))
      (t nil))))

(defun bindings-to-lists (bindings)
  (mapcar (lambda (b) (mapcar #'binding-to-uri b))
          (remove-if (lambda (type) (equal "bnode" type))
                     bindings
                     :key #'cdadar)))

(defun query-to-uri-list (query)
  (let ((values
         (bindings-to-lists
          (query-to-bindings (conf :endpoint) query))))
    (values
     (filter-blacklist
      (filter-whitelist values))
     (length values))))

(defun split-uri (uri)
  "Split URI into stem and resource."
  (let ((pos (+ (position-if (lambda (c) (find c "/#")) uri :from-end t) 1)))
    (values (take pos uri) (subseq uri pos (length uri)))))

(defun uri-resource (uri)
  "Return the resource from a URI."
  (nth-value 1 (split-uri uri)))

(defun looks-like-plist-p (list)
  "Return T if LIST looks like a plist (first element is a keyword)."
  (and (listp list) (keywordp (first list))))

(defun to-json (obj)
  "Return JSON-representation of lisp OBJ."
  (cond
    ((looks-like-plist-p obj)
     (fmt "{~%~{~a: ~a~^,~%~}~%}" (mapcar #'to-json obj)))
    ((listp obj)
     (fmt "[~@[~%~{~a~^, ~}~%~]]" (mapcar #'to-json obj)))
    ((or (stringp obj) (keywordp obj) (symbolp obj))
     (fmt "\"~a\"" obj))
    (t obj)))

(defun resource-to-id (prefix resource)
  (strcat prefix "_" resource))

(defun resource-to-id (uri)
  (multiple-value-bind (stem id) (split-uri uri)
    (strcat (uri-guess-prefix stem) "_" id)))

(defun uri-to-plist (concept-uri)
  (list :|id| (resource-to-id concept-uri)
        :|uri| concept-uri
        :|label| (nth-value 1 (split-uri concept-uri))))

(defun uri-list-to-json (uri-list)
  (let ((resources '()))
    (dolist (uri uri-list)
      (push (uri-to-plist uri) resources))
    (to-json resources)))

(defun concepts-to-json (concepts)
  (uri-list-to-json (mapcar #'concept-uri concepts)))

(defun extract-datatype-properties (concepts)
  (let ((datatype-properties '()))
    (dolist (c concepts)
      (loop for (uri datatype) in (concept-literals c)
            do (pushnew uri datatype-properties :test #'string=)))
    datatype-properties))

(defun datatype-properties-to-json (concepts)
  (uri-list-to-json (extract-datatype-properties concepts)))

(defun extract-object-properties (concepts)
  (let ((object-properties '()))
    (dolist (c concepts)
      (loop for (uri target) in (concept-outgoing-links c)
            do (pushnew uri object-properties :test #'string=))
      (loop for (uri target) in (concept-incoming-links c)
            do (pushnew uri object-properties :test #'string=)))
    object-properties))

(defun object-properties-to-json (concepts)
  (uri-list-to-json (extract-object-properties concepts)))

(defun link-to-plist (link)
  (destructuring-bind (link-name target-type) link
    (list :|propId| (resource-to-id link-name)
          :|target| (resource-to-id target-type))))

(defun uri-to-datatype (uri)
  (subseq uri (+ (position #\# uri) 1) (length uri)))

(defun literal-to-plist (literal)
  (let* ((property-uri (first literal))
         (datatype-uri (second literal))
         (datatype (and datatype-uri (uri-to-datatype datatype-uri)))
         (min-range (third literal))
         (max-range (fourth literal)))
    (append
     (list :|propId| (resource-to-id property-uri))
     (and
      datatype-uri
      (list :|dataType| datatype))
     (and
      min-range max-range
      (cond
        ((equal datatype "date")
         (list
          :|minYear| (parse-integer min-range)
          :|maxYear| (parse-integer max-range)))
        ((equal datatype "integer")
         (list
          :|min| (parse-integer min-range)
          :|max| (parse-integer max-range))))))))

(defun outgoing-links-to-json (concepts)
  (let ((link-list
         (loop for c in concepts collect
           (list :|typeId|
                 (resource-to-id (concept-uri c))
                 :|outgoingLinks|
                 (mapcar #'link-to-plist (concept-outgoing-links c))))))
    (to-json link-list)))

(defun incoming-links-to-json (concepts)
  (let ((link-list
         (loop for c in concepts collect
           (list :|typeId|
                 (resource-to-id (concept-uri c))
                 :|incomingLinks|
                 (mapcar #'link-to-plist (concept-incoming-links c))))))
    (to-json link-list)))

(defun literals-to-json (concepts)
  (let ((literal-list
         (loop for c in concepts
               when (concept-literals c) collect
           (list :|typeId|
                 (resource-to-id (concept-uri c))
                 :|literalValues|
                 (mapcar #'literal-to-plist (concept-literals c))))))
    (to-json literal-list)))

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
        (:datatype-properties
         (list #'datatype-properties-to-json "DatatypeProperties"))
        (:literals
         (list #'literals-to-json "LiteralValues")))
    (when-let ((json (funcall json-printing-function concepts)))
      (format t "var json~a~a = ~a;~%" (conf :dataset-name) name json))))

;; ---------------------------------------------- [ SPARQL-entry inspectors ]
(defun sparql-entry-field (entry)
  (string (first entry)))

(defun sparql-entry-type (entry)
  (cdr (find :type (rest entry) :key #'car)))

(defun sparql-entry-value (entry)
  (cdr (find :value (rest entry) :key #'car)))

;; -------------------------------------------------------------- [ Slurper ]
(defun make-query (section mode &key (limit 0) (offset 0) concept property)
  "MODE should be one of :quick, :full or :paged."
  (ecase section
    (:concepts
     "SELECT DISTINCT ?concept WHERE { ?obj a ?concept . }")
    (:literals
     (fmt
      "SELECT ~a ?prop (DATATYPE(?targetObj) AS ?type) WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
       FILTER (isLiteral(?targetObj)) } ~a"
      (ecase mode (:quick "REDUCED") (:full "DISTINCT") (:paged ""))
      concept
      (if (eq mode :paged) (fmt "LIMIT ~a OFFSET ~a" limit offset) "")))
    (:outgoing-links
     (fmt
      "SELECT ~a ?prop ?targetType WHERE {
         ?obj a <~a> .
         ?obj ?prop ?targetObj .
         ?targetObj a ?targetType . } ~a"
      (ecase mode (:quick "REDUCED") (:full "DISTINCT") (:paged ""))
      concept
      (if (eq mode :paged) (fmt "LIMIT ~a OFFSET ~a" limit offset) "")))
    (:incoming-links
     (fmt
      "SELECT ~a ?prop ?sourceType WHERE {
         ?obj a <~a> .
         ?sourceObj ?prop ?obj . 
         ?sourceObj a ?sourceType . } ~a"
      (ecase mode (:quick "REDUCED") (:full "DISTINCT") (:paged ""))
      concept
      (if (eq mode :paged) (fmt "LIMIT ~a OFFSET ~a" limit offset) "")))
    (:date-limits
     (fmt
      "SELECT (YEAR(MIN(?val)) AS ?minYear) (YEAR(MAX(?val)) AS ?maxYear)
       WHERE {
         ?X0 a <~a> .
         ?X0 <~a> ?val . }"
      concept property))
    (:integer-limits
     (fmt
      "SELECT (MIN(?val) AS ?min) (MAX(?val) AS ?max)
       WHERE {
         ?X0 a <~a> .
         ?X0 <~a> ?val . }"
      concept property))))

(defun repeated-retrieve (section limit page results
                          &key concept property page-limit)
  (fmt-err
   "[page ~a] Retrieving ~a~@[ for ~a~] ... "
   (+ page 1) (string-downcase section) (and concept (uri-resource concept)))
  (let ((offset (* limit page)))
    (handler-case
        (multiple-value-bind (res n)
            (let ((query
                   (make-query
                    section :paged
                    :limit limit
                    :offset offset
                    :concept concept
                    :property property)))
              (query-to-uri-list query))
          (setf results (append res results))
          (fmt-err "ok (found ~a, checked ~a)~%" (length res) n)
          (if (or (/= n limit) (= (+ page 1) page-limit))
              (remove-duplicates results :test #'equal :key #'first)
              (repeated-retrieve
               section limit (+ page 1) results
               :concept concept :property property :page-limit page-limit)))
      (sparql-transaction-time-out ()
        (fmt-err "timeout~%")
        (fmt-err "Retrying in 20 seconds ")
        (sleep 20)
        (repeated-retrieve
         section limit page results
         :concept concept :property property :page-limit page-limit)))))

(defun retrieve (section &optional concept property)
  (fmt-err "[~a] Retrieving ~a~@[ for ~a~] ... "
           (conf :strategy)
           (string-downcase section)
           (and concept (uri-resource concept)))
  (handler-case
      (multiple-value-bind (res n)
          (let ((query
                 (make-query
                  section
                  (string-to-keyword (conf :strategy))
                  :offset 0
                  :concept concept
                  :property property)))
            (query-to-uri-list query))
        (fmt-err "ok (found ~a, checked ~a)~%" (length res) n)
        (remove-duplicates res :test #'equal :key #'first))
    (sparql-transaction-time-out ()
      (fmt-err "timeout~%")
      (fmt-err "Going for paged retrieval ... this may take some time ...~%")
      (repeated-retrieve
       section (parse-integer (conf :results-per-page-limit)) 0 '()
       :concept concept
       :property property
       :page-limit (let ((page-limit (parse-integer (conf :page-limit))))
                    (and (/= page-limit 0) page-limit))))))

(defun get-concept (uri concept-list)
  "Return concept with URI from CONCEPT-LIST."
  (find uri concept-list :key #'concept-uri :test #'string=))

(defun add-link (link-name uri concept link-type)
  "Add a link (LINK-NAME URI) of LINK-TYPE to the CONCEPT's link-list."
  (ecase link-type
    (:incoming-links
     (pushnew
      (list link-name uri) (concept-incoming-links concept) :test #'equal))
    (:outgoing-links
     (pushnew
      (list link-name uri) (concept-outgoing-links concept) :test #'equal))))

(defun add-links (concept link-type concept-list)
  (let* ((mirror-link
          (if (eq link-type :incoming-links) :outgoing-links :incoming-links))
         (links (retrieve link-type (concept-uri concept))))
    (loop for (link-name target-uri) in links do
      (add-link link-name target-uri concept link-type)
      (when-let ((target-concept (get-concept target-uri concept-list)))
        (add-link
         link-name (concept-uri concept) target-concept mirror-link)))))

(defun add-literals (concept)
  (let ((literals (retrieve :literals (concept-uri concept))))
    (setf (concept-literals concept) literals)))

(defun add-literal-limits (concept)
  (dolist (literal (concept-literals concept))
    (let ((type (and (second literal) (uri-to-datatype (second literal)))))
      (cond
        ((equal type "date")
         (appendf
          (rest literal)
          (first
           (retrieve
            :date-limits (concept-uri concept) (first literal)))))
        ((equal type "integer")
         (appendf
          (rest literal)
          (first
           (retrieve
            :integer-limits (concept-uri concept) (first literal)))))))))

(defun slurp (config-file-path)
  (init-config config-file-path)
  (let ((concepts
         (mapcar (lambda (c) (make-concept :uri (first c)))
                 (retrieve :concepts))))
    (dolist (c concepts)
      (add-links c :outgoing-links concepts)
      (add-links c :incoming-links concepts)
      (add-literals c)
      (add-literal-limits c)) 

    (print-json concepts :concepts)
    (print-json concepts :object-properties)
    (print-json concepts :outgoing-links)
    (print-json concepts :incoming-links)
    (print-json concepts :datatype-properties)
    (print-json concepts :literals)))

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
