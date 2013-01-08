(ns tair.core_test
  (refer-clojure :exclude [get namespace])
  (:import [com.taobao.tair TairManager Result DataEntry ResultCode])
  (:use [tair.core])
  (:use [clojure.test]))

;; define the mock tair
(def tair (reify TairManager
            (get [this namespace key]
              (let [result-code (doto (ResultCode. 0 "success"))
                    data-entry (doto (DataEntry.)
                                 (.setKey "foo")
                                 (.setValue "bar"))
                    ret (Result. result-code data-entry)]
                ret))))

;; define the test namespace
(def namespace 99)

(deftest test-get
  (is (= "bar" (get tair namespace "key"))))
