(ql:quickload :lisp-unit)

(unless (find-package :sparql-endpoint-analyzer)
  (load "sparql-endpoint-analyzer.lisp"))

(in-package :sparql-endpoint-analyzer)
(shadow :results :sparql-endpoint-analyzer)
(use-package :lisp-unit)

(define-test prettify-label
  (assert-equal "lowercase" (prettify-label "lowercase"))
  (assert-equal "Camel case" (prettify-label "CamelCase"))
  (assert-equal "CAPS then camel" (prettify-label "CAPSThenCamel"))
  (assert-equal "lower case" (prettify-label "lower_case"))
  (assert-equal "Camel Case" (prettify-label "Camel_Case"))
  (assert-equal "CAPS Then Camel" (prettify-label "CAPS_Then_Camel")))

(define-test split-uri
  (assert-equal
   "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   (uri-stem "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
  (assert-equal
   "type"
   (uri-resource "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
  (assert-equal
   "http://data.computas.com/informasjonsmodell/organisasjon/"
   (uri-stem
  "http://data.computas.com/informasjonsmodell/organisasjon/Enhet"))
  (assert-equal
   "Enhet"
   (uri-resource "http://data.computas.com/informasjonsmodell/organisasjon/Enhet")))

(define-test to-json
  (assert-equal
   "{
\"A\": 1,
\"B\": \"two\",
\"C\": 3
}"
   (to-json '(:a 1 :b "two" :c 3)))
  (assert-equal
   "[
1, \"two\", 3
]"
   (to-json '(1 "two" 3)))
  (assert-equal 42 (to-json 42))
  (assert-equal 3.14 (to-json 3.14))
  (assert-equal "true" (to-json 'true))
  (assert-equal "false" (to-json 'false))
  (assert-equal "\"string\"" (to-json "string"))
  (assert-equal "\"SYMBOL\"" (to-json 'symbol))
  (assert-equal "\"KEYWORD\"" (to-json :keyword)))

(define-test util
  (assert-equal "XLII" (fmt "~@R" 42))
  (assert-true (looks-like-plist-p '(:a 1 :b 2 :c 3)))
  (assert-false (looks-like-plist-p '(1 :a 2 3 :b)))
  (assert-true
   (looks-like-uri-p
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
  (assert-false (looks-like-uri-p "22-rdf-syntax-ns#type"))
  (assert-equal "     " (spaces 5))
  (assert-equal "concatenateme" (strcat "con" "cat" "en" "ate" "me"))
  (assert-true
   (string-prefix-p
    "http://www.w3.org/2000/01/"
    "http://www.w3.org/2000/01/rdf-schema#label"))
  (assert-false
   (string-prefix-p
    "http://www.w3.org/2000/01/"
    "http://www.w3.org/2002/07/owl#Class"))
  (assert-equal :KEYWORD (string-to-keyword "keyword"))
  (assert-equal '(a b c) (take 3 '(a b c d e)))
  (assert-equal '(a b) (take 3 '(a b))))

(define-test xsd-year
  (assert-equal "2014" (xsd-year "2014-05-22")) ; xsd:date
  (assert-equal "2014" (xsd-year "2014-05-22T17:05:53")) ; xsd:dateTime
  (assert-equal "2014" (xsd-year "2014")) ; xsd:gYear
  (assert-equal "2014" (xsd-year "2014-05"))) ; xsd:gYearMonth

(let ((*print-failures* t))
  (run-tests :all))