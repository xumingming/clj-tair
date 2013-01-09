(ns tair.core
  (:refer-clojure :exclude [get])
  (:import [com.taobao.tair TairManager ResultCode CallMode]
           [com.taobao.tair.impl.mc MultiClusterTairManager]
           [com.taobao.tair.etc KeyValuePack]
           [com.alibaba.fastjson JSON]
           [java.net URL]
           [java.util Map List])
  (:require [clojure.walk :as walk]))

(declare pretify-result clojurify-result-code)

(defn mk-tair
  "Create a tair instance."
  ([config-id]
     (mk-tair config-id true))
  ([config-id dynamic?]
     (doto (MultiClusterTairManager.) (.setConfigID config-id)
           (.setDynamicConfig dynamic?)
           (.init))))

(defn get
  "Query the value of the specified key from the specified namespace."
  [^TairManager tair namespace key]
  (let [obj (.get tair namespace key)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue)))
                     (not (nil? (-> obj .getValue .getValue))))
              (-> obj .getValue
                  .getValue
                  pretify-result)
              nil)]
    obj))

(defn mget
  "Batch get data from tair

  tair: the tair manager
  namespace: the namespace where the data is in
  keys: a collection of keys to get"
  [^TairManager tair namespace keys]
  (let [ret (.mget tair namespace (vec keys))
        ret (if (and (not (nil? ret))
                     (not (nil? (-> ret .getValue))))
              (into {} (map #(vector (.getKey %) (.getValue %)) (.getValue ret)))
              nil)
        ]
    ret))

(defn put
  "Put the key value pair into the specified namespace with the specified expiretime.

  If 0 is specified for the expiretime, it means it will not expire
  If fill-cache? is true, then it will also insert the data into cache (only
  applies when server-side support cache."
  ([tair namespace key value]
     (put tair namespace key value 0))
  ([tair namespace key value version]
     (put tair namespace key value version 0))
  ([tair namespace key value version expire-time]
     (put tair namespace key value version 0 true))
  ([tair namespace key value version expire-time fill-cache?]
     (let [result-code (.put tair namespace key value version expire-time fill-cache?)
           result-code (clojurify-result-code result-code)]
       result-code)))

(defn put-async
  [tair namespace key value version expire-time fill-cache? callback]
  nil)

(defn delete
  "Delete the specified key from tair"
  [tair namespace key]
  (let [result-code (.delete tair namespace key)
        result-code (clojurify-result-code result-code)]
    result-code))

(defn invalid
  "
  callmode can be one of [:sync :async]"
  ([tair namespace key]
     (invalid tair namespace key :sync))
  ([tair namespace key callmode]
     (let [callmode (if (= :sync callmode)
                      CallMode/SYNC
                      CallMode/ASYNC)
           result-code (.invalid tair namespace key callmode)]
       (clojurify-result-code result-code))))

(defn hide
  [tair namespace key]
  (let [result-code (.hide tair namespace key)]
    (clojurify-result-code result-code)))

(defn hide-by-proxy
  ([tair namespace key]
     (hide-by-proxy tair namespace key :sync))
  ([tair namespace key callmode]
     (let [callmode (if (= :sync callmode)
                      CallMode/SYNC
                      CallMode/ASYNC)
           result-code (.hideByProxy tair namespace key callmode)]
       (clojurify-result-code result-code))))

(defn get-hidden
  [tair namespace key]
  (let [obj (.getHidden tair namespace key)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue)))
                     (not (nil? (-> obj .getValue .getValue))))
              (-> obj .getValue
                  .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-put
  ([tair namespace pkey skey value]
     (prefix-put tair namespace pkey skey value 0))
  ([tair namespace pkey skey value version]
     (prefix-put tair namespace pkey skey value version 0))
  ([tair namespace pkey skey value version expire-time]
     (let [result-code (.prefixPut tair namespace pkey skey value version expire-time)]
       (clojurify-result-code result-code))))

(defn prefix-puts
  [tair namespace pkey skey-value-packs]
  (let [skey-value-packs (map (fn [key-value-pack]
                                (case (count key-value-pack)
                                  2 (KeyValuePack. (first key-value-pack) (second key-value-pack))
                                  3 (KeyValuePack. (first key-value-pack) (second key-value-pack) (nth key-value-pack 2))
                                  4 (KeyValuePack. (first key-value-pack) (second key-value-pack) (nth key-value-pack 2) (nth key-value-pack 3)))) skey-value-packs)
        result (.prefixPuts tair namespace pkey skey-value-packs)
        result (if (and (not (nil? result))
                        (not (nil? (.getValue result))))
                 (into {} (map #(vector (first %) (clojurify-result-code (second %))) (.getValue result)))
                 {})]
    result))

(defn prefix-delete
  [tair namespace pkey skey]
  (let [result-code (.prefixDelete tair namespace pkey skey)]
    (clojurify-result-code result-code)))

(defn prefix-deletes
  [tair namespace pkey skeys]
  (let [result (.prefixDeletes tair namespace pkey skeys)
        result (if (and (not (nil? result))
                        (not (nil? (.getValue result))))
                 (into {} (map #(vector (first %) (clojurify-result-code (second %))) (.getValue result)))
                 {})]
    result))

(defn- object-to-json [obj]
  (JSON/toJSON obj))

(defn clojurify-structure [s]
  (walk/prewalk (fn [x]
              (cond (instance? Map x) (into {} x)
                    (instance? List x) (vec x)
                    true x))
           s))

(defn clojurify-result-code [^ResultCode result-code]
  {:code (.getCode result-code)
   :message (.getMessage result-code)})

(defn pretify-result [obj]
  (-> obj object-to-json clojurify-structure))

