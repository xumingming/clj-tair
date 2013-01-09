(ns tair.core_test
  (refer-clojure :exclude [get namespace])
  (:import [com.taobao.tair TairManager Result DataEntry ResultCode])
  (:import [java.io Serializable])
  (:use [tair.core])
  (:use [clojure.test]))

;; define the mock tair
(def tair (reify TairManager
            (get [this namespace key]
              (let [result-code (ResultCode. 0 "success")
                    data-entry (doto (DataEntry.)
                                 (.setKey "foo")
                                 (.setValue "bar"))
                    ret (Result. result-code data-entry)]
                ret))
            (^ResultCode put [this ^int namespace ^Serializable key ^Serializable value
                              ^int version ^int expire-time ^boolean fill-cache?]
              (let [^ResultCode result-code (ResultCode. 0 "success")]
                result-code))
            (mget [this namespace keys]
              (let [data-entries [(DataEntry. "foo" "bar")
                                  (DataEntry. "hello" "world")]
                    result-code (ResultCode. 0 "success")
                    ret (Result. result-code data-entries)]
                ret))
            (delete [this namespace key]
              (ResultCode. 0 "success"))
            (invalid [this namespace key callmode]
              (ResultCode. 0 "success"))
            (hide [this namespace key]
              (ResultCode. 0 "success"))
            (hideByProxy [this namespace key callmode]
              (ResultCode. 0 "success"))
            (getHidden [this namespace key]
              (let [result-code (ResultCode. 0 "success")
                    data-entry (doto (DataEntry.)
                                 (.setKey "foo")
                                 (.setValue "bar"))
                    ret (Result. result-code data-entry)]
                ret))
            (prefixPut [this namespace pkey skey value version expire-time]
              (ResultCode. 0 "success"))
            (prefixPuts [this namespace pkey skey-value-packs]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code {"bar1" result-code
                                              "bar2" result-code
                                              "bar3" result-code})]
                ret))
            (prefixDelete [this namespace pkey skey]
              (ResultCode. 0 "success"))
            (prefixDeletes [this namespace pkey skeys]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code {"bar1" result-code
                                              "bar2" result-code})]
                ret))
            (prefixGet [this namespace pkey skey]
              (let [result-code (ResultCode. 0 "success")
                    data-entry (doto (DataEntry.)
                                 (.setKey "foo")
                                 (.setValue "bar"))
                    ret (Result. result-code data-entry)]
                ret))
            (prefixGets [this namespace pkey skeys]
              (let [data-entries [(DataEntry. "bar1" "value1")
                                  (DataEntry. "bar2" "value2")]
                    result-code (ResultCode. 0 "success")
                    ret (Result. result-code data-entries)]
                ret))
            (prefixIncr
              [this namespace pkey skey value default-value expire-time]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code (int 100))]
                ret))
            (prefixIncrs
              [this namespace pkey skey-packs]
              (let [result-code (ResultCode. 0 "success")
                    inner-ret (Result. result-code (int 100))
                    out-ret (Result. result-code {"bar1" inner-ret "bar2" inner-ret})]
                out-ret))
            (prefixDecr
              [this namespace pkey skey value default-value expire-time]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code (int 100))]
                ret))
            (prefixDecrs
              [this namespace pkey skey-packs]
              (let [result-code (ResultCode. 0 "success")
                    inner-ret (Result. result-code (int 100))
                    out-ret (Result. result-code {"bar1" inner-ret "bar2" inner-ret})]
                out-ret))
            (prefixSetCount
              [this namespace pkey skey count version expire-time]
              (ResultCode. 0 "success"))
            (prefixHide
              [this namespace pkey skey]
              (ResultCode. 0 "success"))
            (prefixHides
              [this namespace pkey skeys]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code {"bar1" result-code "bar2" result-code})]
                ret))
            (prefixGetHidden
              [this namespace pkey skey]
              (let [result-code (ResultCode. 0 "success")
                    data-entry (doto (DataEntry.)
                                 (.setKey "foo")
                                 (.setValue "bar"))
                    ret (Result. result-code data-entry)]
                ret))
            (prefixGetHiddens
              [this namespace pkey skeys]
              (let [result-code (ResultCode. 0 "success")
                    inner-ret (Result. result-code "value")
                    out-ret (Result. result-code {"bar1" inner-ret "bar2" inner-ret})]
                out-ret))
            (prefixInvalid
              [this namespace pkey skey callmode]
              (ResultCode. 0 "success"))
            (prefixInvalids
              [this namespace pkey skeys callmode]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code {"bar1" result-code
                                              "bar2" result-code})]
                ret))
            (prefixHideByProxy
              [this namespace pkey skey callmode]
              (ResultCode. 0 "success"))
            (prefixHidesByProxy
              [this namespace pkey skeys callmode]
              (let [result-code (ResultCode. 0 "success")
                    ret (Result. result-code {"bar1" result-code
                                              "bar2" result-code})]
                ret))
            (mprefixGetHiddens
              [tair namespace p-s-keys]
              (let [result-code (ResultCode. 0 "success")
                    data-entry (DataEntry. "foo" "value")
                    temp-result (Result. result-code data-entry)
                    ret (Result. result-code {"foo1" {"bar1" temp-result
                                                      "bar2" temp-result}
                                              "foo2" {"bar3" temp-result
                                                      "bar4" temp-result}})]
                ret))
            (munlock
              [this namespace keys]
              (Result. ResultCode/SUCCESS []))
            (munlock
              [this namespace keys fail-keys-map]
              (.put fail-keys-map "bar" ResultCode/TIMEOUT)
              (Result. ResultCode/PARTSUCC ["foo"]))
            (getStat
              [this qtype group-name server-id]
              {"foo" "bar" "foo1" "bar1"})
            (getVersion
              [this]
              "1.0.0")
            (getMaxFailCount
              [this]
              (int 99))
            (setMaxFailCount
              [this cnt]
              nil)))

;; define the test namespace
(def namespace 99)

;; success result code
(def success-result-code (clojurify-result-code ResultCode/SUCCESS))

;; part success result code
(def part-success-result-code (clojurify-result-code ResultCode/PARTSUCC))

(deftest test-get
  (is (= "bar" (get tair namespace "key"))))

(deftest test-mget
  (is (= {"foo" "bar" "hello" "world"} (mget tair namespace ["foo" "hello"]))))

(deftest test-put
  (is (= success-result-code (put tair namespace "foo" "bar")))
  (is (= success-result-code (put tair namespace "foo" "bar" 0)))
  (is (= success-result-code (put tair namespace "foo" "bar" 0 0)))
  (is (= success-result-code (put tair namespace "foo" "bar" 0 0 false))))

(deftest test-delete
  (is (= success-result-code (delete tair namespace "foo"))))

(deftest test-invalid
  (is (= success-result-code (invalid tair namespace "foo")))
  (is (= success-result-code (invalid tair namespace "foo" :async))))

(deftest test-hide
  (is (= success-result-code (hide tair namespace "foo"))))

(deftest test-hide-by-proxy
  (is (= success-result-code (hide-by-proxy tair namespace "foo"))))

(deftest test-get-hidden
  (is (= "bar" (get-hidden tair namespace "foo"))))

(deftest test-prefix-put
  (is (= success-result-code (prefix-put tair namespace "foo" "bar" "value")))
  (is (= success-result-code (prefix-put tair namespace "foo" "bar" "value" 0)))
  (is (= success-result-code (prefix-put tair namespace "foo" "bar" "value" 0 0))))

(deftest test-prefix-puts
  (is (= {"bar1" success-result-code
          "bar2" success-result-code
          "bar3" success-result-code}
         (prefix-puts tair namespace "foo" [["bar1" "value1"]
                                            ["bar2" "value2" 0]
                                            ["bar3" "value3" 0 0]]))))

(deftest test-prefix-delete
  (is (= success-result-code (prefix-delete  tair namespace "foo" "bar"))))

(deftest test-prefix-deletes
  (is (= {"bar1" success-result-code
          "bar2" success-result-code}
         (prefix-deletes tair namespace "foo" ["bar1" "bar2"]))))

(deftest test-prefix-get
  (is (= "bar" (prefix-get tair namespace "foo" "bar"))))

(deftest test-prefix-gets
  (is (= {"bar1" "value1" "bar2" "value2"} (prefix-gets  tair namespace "foo" ["bar1" "bar2"]))))

(deftest test-prefix-incr
  (is (= 100 (prefix-incr tair namespace "foo" "bar" 1 0 0))))

(deftest test-prefix-incrs
  (is (= {"bar1" 100 "bar2" 100} (prefix-incrs tair namespace "foo" [["bar1" 1 0 0] ["bar2" 1 0 0]]))))

(deftest test-prefix-decr
  (is (= 100 (prefix-decr tair namespace "foo" "bar" 1 0 0))))

(deftest test-prefix-decrs
  (is (= {"bar1" 100 "bar2" 100} (prefix-decrs tair namespace "foo" [["bar1" 1 0 0] ["bar2" 1 0 0]]))))

(deftest test-prefix-set-count
  (is (= success-result-code (prefix-set-count tair namespace "foo" "bar" (int 10)))))

(deftest test-prefix-hide
  (is (= success-result-code (prefix-hide tair namespace "foo" "bar"))))

(deftest test-prefix-hides
  (is (= {"bar1" success-result-code "bar2" success-result-code}
         (prefix-hides tair namespace "foo" ["bar1" "bar2"]))))

(deftest test-prefix-get-hidden
  (is (= "bar" (prefix-get-hidden tair namespace "foo" "bar"))))

(deftest test-prefix-get-hiddens
  (is (= {"bar1" "value" "bar2" "value"} (prefix-get-hiddens tair namespace "foo" ["bar1" "bar2"]))))

(deftest test-prefix-invalid
  (is (= success-result-code (prefix-invalid tair namespace "foo" "bar" :sync))))

(deftest test-prefix-invalids
  (is (= {"bar1" success-result-code
          "bar2" success-result-code}
         (prefix-invalids tair namespace "foo" ["bar1" "bar2"] :sync))))

(deftest test-prefix-hide-by-proxy
  (is (= success-result-code
         (prefix-hide-by-proxy tair namespace "foo" "bar" :sync))))

(deftest test-prefix-hides-by-proxy
  (is (= {"bar1" success-result-code
          "bar2" success-result-code}
         (prefix-hides-by-proxy tair namespace "foo" ["bar1" "bar2"] :sync))))

(deftest test-mprefix-get-hiddens
  (is (= {"foo1" {"bar1" "value"
                  "bar2" "value"}
          "foo2" {"bar3" "value"
                  "bar4" "value"}}
         (mprefix-get-hiddens tair namespace {"foo1" ["bar1" "bar2"] "foo2" ["bar3" "bar4"]}))))

(deftest test-munlock
  (is (= {:rc part-success-result-code
          :data ["foo"]
          :fail-keys-map {"bar" (clojurify-result-code ResultCode/TIMEOUT)}}
         (munlock tair namespace ["foo" "bar"] true)))
  (is (= {:rc success-result-code
          :data []
          :fail-keys-map {}}
         (munlock tair namespace ["hello" "world"]))))

(deftest test-get-stat
  (is (= {"foo" "bar" "foo1" "bar1"}
         (get-stat tair 1 "group-name" 1))))

(deftest test-get-version
  (is (= "1.0.0" (get-version tair))))

(deftest test-get-max-fail-count
  (is (= (int 99) (get-max-fail-count tair))))

(deftest test-set-max-fail-count
  (set-max-fail-count tair (int 99)))