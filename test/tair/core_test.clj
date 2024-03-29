(ns tair.core_test
  (refer-clojure :exclude [get namespace])
  (:import [com.taobao.tair TairManager Result DataEntry ResultCode])
  (:import [java.io Serializable])
  (:require [tair.core :refer :all])
  (:require [clojure.test :refer :all]))

;; define the mock tair
(def tair (reify TairManager
            (get [this namespace key]
              (Result. ResultCode/SUCCESS (doto (DataEntry.)
                                            (.setKey "foo")
                                            (.setValue "bar"))))
            (put [this namespace key value
                  version expire-time fill-cache?]
              ResultCode/SUCCESS)
            (putAsync [this namespace key value
                  version expire-time fill-cache? tair-callback]
              ResultCode/SUCCESS)
            (mget [this namespace keys]
              (Result. ResultCode/SUCCESS [(DataEntry. "foo" "bar")
                                           (DataEntry. "hello" "world")]))
            (delete [this namespace key]
              ResultCode/SUCCESS)
            (invalid [this namespace key callmode]
              ResultCode/SUCCESS)
            (hide [this namespace key]
              ResultCode/SUCCESS)
            (hideByProxy [this namespace key callmode]
              ResultCode/SUCCESS)
            (getHidden [this namespace key]
              (Result. ResultCode/SUCCESS (doto (DataEntry.)
                                            (.setKey "foo")
                                            (.setValue "bar"))))

            (prefixPut [this namespace pkey skey value version expire-time]
              ResultCode/SUCCESS)
            (prefixPuts [this namespace pkey skey-value-packs]
              (Result. ResultCode/SUCCESS {"bar1" ResultCode/SUCCESS
                                           "bar2" ResultCode/SUCCESS
                                           "bar3" ResultCode/SUCCESS}))
            (prefixDelete [this namespace pkey skey]
              ResultCode/SUCCESS)
            (prefixDeletes [this namespace pkey skeys]
              (Result. ResultCode/SUCCESS {"bar1" ResultCode/SUCCESS
                                           "bar2" ResultCode/SUCCESS}))
            (prefixGet [this namespace pkey skey]
              (Result. ResultCode/SUCCESS (doto (DataEntry.)
                                            (.setKey "foo")
                                            (.setValue "bar"))))
            (prefixGets [this namespace pkey skeys]
              (Result. ResultCode/SUCCESS {"bar1" (Result. ResultCode/SUCCESS (DataEntry. "bar1" "value1"))
                                           "bar2" (Result. ResultCode/SUCCESS (DataEntry. "bar2" "value2"))}))
            (prefixIncr
              [this namespace pkey skey value default-value expire-time]
              (Result. ResultCode/SUCCESS (int 100)))
            (prefixIncrs
              [this namespace pkey skey-packs]
              (Result. ResultCode/SUCCESS
                       {"bar1" (Result. ResultCode/SUCCESS (int 100))
                        "bar2" (Result. ResultCode/SUCCESS (int 100))}))
            (prefixDecr
              [this namespace pkey skey value default-value expire-time]
              (Result. ResultCode/SUCCESS (int 100)))
            (prefixDecrs
              [this namespace pkey skey-packs]
              (Result. ResultCode/SUCCESS
                       {"bar1" (Result. ResultCode/SUCCESS (int 100))
                        "bar2" (Result. ResultCode/SUCCESS (int 100))}))
            (prefixSetCount
              [this namespace pkey skey count version expire-time]
              ResultCode/SUCCESS)
            (prefixHide
              [this namespace pkey skey]
              ResultCode/SUCCESS)
            (prefixHides
              [this namespace pkey skeys]
              (Result. ResultCode/SUCCESS
                       {"bar1" ResultCode/SUCCESS
                        "bar2" ResultCode/SUCCESS}))
            (prefixGetHidden
              [this namespace pkey skey]
              (Result. ResultCode/SUCCESS
                       (doto (DataEntry.)
                         (.setKey "foo")
                         (.setValue "bar"))))
            (prefixGetHiddens
              [this namespace pkey skeys]
              (Result. ResultCode/SUCCESS
                       {"bar1" (Result. ResultCode/SUCCESS (DataEntry. ResultCode/SUCCESS "value")) 
                        "bar2" (Result. ResultCode/SUCCESS (DataEntry. ResultCode/SUCCESS "value"))}))
            (prefixInvalid
              [this namespace pkey skey callmode]
              ResultCode/SUCCESS)
            (prefixInvalids
              [this namespace pkey skeys callmode]
              (Result. ResultCode/SUCCESS
                       {"bar1" ResultCode/SUCCESS
                        "bar2" ResultCode/SUCCESS}))
            (prefixHideByProxy
              [this namespace pkey skey callmode]
              ResultCode/SUCCESS)
            (prefixHidesByProxy
              [this namespace pkey skeys callmode]
              (Result. ResultCode/SUCCESS
                       {"bar1" ResultCode/SUCCESS
                        "bar2" ResultCode/SUCCESS}))
            (mprefixGetHiddens
              [tair namespace p-s-keys]
              (Result. ResultCode/SUCCESS
                       {"foo1" {"bar1" (Result. ResultCode/SUCCESS (DataEntry. "foo" "value"))
                                "bar2" (Result. ResultCode/SUCCESS (DataEntry. "foo" "value"))}
                        "foo2" {"bar3" (Result. ResultCode/SUCCESS (DataEntry. "foo" "value"))
                                "bar4" (Result. ResultCode/SUCCESS (DataEntry. "foo" "value"))}}))
            (minvalid
              [tair namespace keys]
              ResultCode/SUCCESS)
            (mdelete
              [tair namespace keys]
              ResultCode/SUCCESS)
            (getRange
              [tair namespace prefix key-start key-end offset limit]
              (Result. ResultCode/SUCCESS
                       [(DataEntry. "foo" "bar")
                        (DataEntry. "hello" "world")]))
            (getRangeOnlyKey
              [tair namespace prefix key-start key-end offset limit]
              (Result. ResultCode/SUCCESS [(DataEntry. "foo" "bar")
                                           (DataEntry. "hello" "world")]))
            (getRangeOnlyValue
              [tair namespace prefix key-start key-end offset limit]
              (Result. ResultCode/SUCCESS [(DataEntry. "foo" "bar")
                                           (DataEntry. "hello" "world")]))
            (incr
              [tair namespace key value default-value expire-time]
              (Result. ResultCode/SUCCESS 99))
            (decr
              [tair namespace key value default-value expire-time]
              (Result. ResultCode/SUCCESS 99))
            (setCount
              [tair namespace key count]
              ResultCode/SUCCESS)
            (setCount
              [tair namespace key count version expire-time]
              ResultCode/SUCCESS)
            (addItems
              [tair namespace key items max-count version expire-time]
              ResultCode/SUCCESS)
            (getItems
              [tair namespace key offset count]
              (Result. ResultCode/SUCCESS (DataEntry. 1)))
            (removeItems
              [tair namespace key offset count]
              ResultCode/SUCCESS)
            (getAndRemove
              [tair namespace key offset count]
              (Result. ResultCode/SUCCESS (DataEntry. 1)))
            (getItemCount
              [tair namespace key]
              (Result. ResultCode/SUCCESS 99))
            (lock
              [tair namespace key]
              ResultCode/SUCCESS)
            (unlock
              [tair namespace key]
              ResultCode/SUCCESS)
            (mlock
              [this namespace keys]
              (Result. ResultCode/SUCCESS []))
            (mlock
              [this namespace keys fail-keys-map]
              (.put fail-keys-map "bar" ResultCode/TIMEOUT)
              (Result. ResultCode/PARTSUCC ["foo"]))
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
  (is (= '(["foo" "bar"] ["hello" "world"]) (mget tair namespace ["foo" "hello"]))))

(deftest test-put
  (is (= success-result-code (put tair namespace "foo" "bar")))
  (is (= success-result-code (put tair namespace "foo" "bar" 0)))
  (is (= success-result-code (put tair namespace "foo" "bar" 0 0)))
  (is (= success-result-code (put tair namespace "foo" "bar" 0 0 false))))

(deftest test-put-async
  (is (= success-result-code (put-async tair namespace "foo" "bar" 0 0 false nil nil))))

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

(deftest test-minvalid
  (is (= success-result-code (minvalid tair namespace ["foo"]))))

(deftest test-mdelete
  (is (= success-result-code (mdelete tair namespace ["foo"]))))

(deftest test-get-range
  (is (= '(["foo" "bar"] ["hello" "world"]) (get-range tair namespace "foo" "1" "2" 1 1))))

(deftest test-get-range-only-key
  (is (= '("foo" "hello") (get-range-only-key tair namespace "foo" "1" "2" 1 1))))

(deftest test-get-range-only-value
  (is (= '("bar" "world") (get-range-only-value tair namespace "foo" "1" "2" 1 1))))

(deftest test-mlock
  (is (= {:rc part-success-result-code
          :data ["foo"]
          :fail-keys-map {"bar" (clojurify-result-code ResultCode/TIMEOUT)}}
         (mlock tair namespace ["foo" "bar"] true)))
  (is (= {:rc success-result-code
          :data []
          :fail-keys-map {}}
         (mlock tair namespace ["hello" "world"]))))

(deftest test-incr
  (is (= 99 (incr tair namespace "foo" 1 0 0))))

(deftest test-decr
  (is (= 99 (decr tair namespace "foo" 1 0 0))))

(deftest test-set-count
  (is (= success-result-code (set-count tair namespace "foo" 1)))
  (is (= success-result-code (set-count tair namespace "foo" 1 1 1))))

(deftest test-add-items
  (is (= success-result-code (add-items tair namespace "foo" ["foo" "bar"] 1 1 1))))

(deftest test-get-items
  (is (= 1 (get-items tair namespace "foo" 1 1))))

(deftest test-remove-items
  (is (= success-result-code (remove-items tair namespace "foo" 1 1))))

(deftest test-get-and-remove
  (is (= 1 (get-and-remove tair namespace "foo" 1 2))))

(deftest test-get-item-count
  (is (= 99 (get-item-count tair namespace "foo"))))

(deftest test-lock
  (is (= success-result-code (lock tair namespace "foo"))))

(deftest test-unlock
  (is (= success-result-code (unlock tair namespace "foo"))))

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