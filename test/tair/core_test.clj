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
                ret))))

;; define the test namespace
(def namespace 99)

(deftest test-get
  (is (= "bar" (get tair namespace "key"))))

(deftest test-mget
  (is (= {"foo" "bar" "hello" "world"} (mget tair namespace ["foo" "hello"]))))

(deftest test-put
  (is (= {:code 0 :message "success"} (put tair namespace "foo" "bar")))
  (is (= {:code 0 :message "success"} (put tair namespace "foo" "bar" 0)))
  (is (= {:code 0 :message "success"} (put tair namespace "foo" "bar" 0 0)))
  (is (= {:code 0 :message "success"} (put tair namespace "foo" "bar" 0 0 false))))

(deftest test-delete
  (is (= {:code 0 :message "success"} (delete tair namespace "foo"))))

(deftest test-invalid
  (is (= {:code 0 :message "success"} (invalid tair namespace "foo")))
  (is (= {:code 0 :message "success"} (invalid tair namespace "foo" :async))))

(deftest test-hide
  (is (= {:code 0 :message "success"} (hide tair namespace "foo"))))

(deftest test-hide-by-proxy
  (is (= {:code 0 :message "success"} (hide-by-proxy tair namespace "foo"))))

(deftest test-get-hidden
  (is (= "bar" (get-hidden tair namespace "key"))))

(deftest test-prefix-put
  (is (= {:code 0 :message "success"} (prefix-put tair namespace "foo" "bar" "value")))
  (is (= {:code 0 :message "success"} (prefix-put tair namespace "foo" "bar" "value" 0)))
  (is (= {:code 0 :message "success"} (prefix-put tair namespace "foo" "bar" "value" 0 0))))

(deftest test-prefix-puts
  (is (= {"bar1" {:code 0 :message "success"}
          "bar2" {:code 0 :message "success"}
          "bar3" {:code 0 :message "success"}}
         (prefix-puts tair namespace "foo" [["bar1" "value1"]
                                            ["bar2" "value2" 0]
                                            ["bar3" "value3" 0 0]]))))

(deftest test-prefix-delete
  (is (= {:code 0 :message "success"} (prefix-delete  tair namespace "foo" "bar"))))

(deftest test-prefix-deletes
  (is (= {"bar1" {:code 0 :message "success"}
          "bar2" {:code 0 :message "success"}}
         (prefix-deletes tair namespace "foo" ["bar1" "bar2"]))))
