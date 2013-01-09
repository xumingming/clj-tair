(ns tair.core
  (:refer-clojure :exclude [get])
  (:import [com.taobao.tair TairManager ResultCode CallMode]
           [com.taobao.tair.impl.mc MultiClusterTairManager]
           [com.taobao.tair.etc KeyValuePack CounterPack]
           [com.alibaba.fastjson JSON]
           [java.net URL]
           [java.util Map HashMap List])
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

(defn prefix-get
  [tair namespace pkey skey]
  (let [obj (.prefixGet tair namespace pkey skey)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue)))
                     (not (nil? (-> obj .getValue .getValue))))
              (-> obj .getValue
                  .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-gets
  [tair namespace pkey skeys]
  (let [ret (.prefixGets tair namespace pkey (vec skeys))
        ret (if (and (not (nil? ret))
                     (not (nil? (-> ret .getValue))))
              (into {} (map #(vector (.getKey %) (.getValue %)) (.getValue ret)))
              nil)
        ]
    ret))

(defn prefix-incr
  [tair namespace pkey skey value default-value expire-time]
  (let [obj (.prefixIncr tair namespace pkey skey value default-value expire-time)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue))))
              (-> obj .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-incrs
  [tair namespace pkey skey-packs]
  (let [skey-packs (map (fn [key-pack]
                                (case (count key-pack)
                                  3 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2))
                                  4 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2) (nth key-pack 3)))) skey-packs)
        result (.prefixIncrs tair namespace pkey skey-packs)
        result (if (and (not (nil? result))
                        (not (nil? (.getValue result))))
                 (into {} (map #(vector (first %) (.getValue (second %))) (.getValue result)))
                 {})]
    result))

(defn prefix-decr
  [tair namespace pkey skey value default-value expire-time]
  (let [obj (.prefixDecr tair namespace pkey skey value default-value expire-time)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue))))
              (-> obj .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-decrs
  [tair namespace pkey skey-packs]
  (let [skey-packs (map (fn [key-pack]
                                (case (count key-pack)
                                  3 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2))
                                  4 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2) (nth key-pack 3)))) skey-packs)
        result (.prefixDecrs tair namespace pkey skey-packs)
        result (if (and (not (nil? result))
                        (not (nil? (.getValue result))))
                 (into {} (map #(vector (first %) (.getValue (second %))) (.getValue result)))
                 {})]
    result))

(defn prefix-set-count
  ([tair namespace pkey skey count]
     (prefix-set-count tair namespace pkey skey count 0 0))
  ([tair namespace pkey skey count version expire-time]
     (let [result-code (.prefixSetCount tair namespace pkey skey count version expire-time)]
       (clojurify-result-code result-code))))

(defn prefix-hide
  [tair namespace pkey skey]
  (let [result-code (.prefixHide tair namespace pkey skey)]
    (clojurify-result-code result-code)))

(defn prefix-hides
  [tair namespace pkey skeys]
  (let [ret (.prefixHides tair namespace pkey skeys)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              {})
        ret (into {} (map #(vector (first %) (clojurify-result-code (second %))) ret))]
    ret))

(defn prefix-get-hidden
  [tair namespace pkey skey]
  (let [ret (.prefixGetHidden tair namespace pkey skey)
        ret (if (and (not (nil? ret))
                     (not (nil? (-> ret .getValue)))
                     (not (nil? (-> ret .getValue .getValue))))
              (-> ret .getValue
                  .getValue
                  pretify-result)
              nil)]
    ret))

(defn prefix-get-hiddens
  [tair namespace pkey skeys]
  (let [ret (.prefixGetHiddens tair namespace pkey (vec skeys))
        ret (if (and (not (nil? ret))
                     (not (nil? (-> ret .getValue))))
              (into {} (map #(vector (.getKey %) (-> % .getValue .getValue pretify-result)) (.getValue ret)))
              nil)
        ]
    ret))

(defn prefix-invalid
  [tair namespace pkey skey callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        result-code (.prefixInvalid tair namespace pkey skey callmode)]
    (clojurify-result-code result-code)))

(defn prefix-invalids
  [tair namespace pkey skeys callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        ret (.prefixInvalids tair namespace pkey skeys callmode)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (into {} (map #(vector (first %) (clojurify-result-code (second %)))
                            (.getValue ret))))]
    ret))

(defn prefix-hide-by-proxy
  [tair namespace pkey skey callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        result-code (.prefixHideByProxy tair namespace pkey skey callmode)]
    (clojurify-result-code result-code)))

(defn prefix-hides-by-proxy
  [tair namespace pkey skeys callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        ret (.prefixHidesByProxy tair namespace pkey skeys callmode)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))

              (into {} (map #(vector (first %) (clojurify-result-code (second %)))
                            (.getValue ret)))
              {})]
    ret))

(defn mprefix-get-hiddens
  [tair namespace p-s-keys]
  (let [ret (.mprefixGetHiddens tair namespace p-s-keys)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              {})
        ret (into {} (map (fn [[pkey values]]
                            [pkey
                             (into {} (map #(vector (first %) (-> (second %) .getValue .getValue))
                                           values))])
                          ret))]
    ret))

;; TODO miss unit-test here.
(defn minvalid
  [tair namespace keys]
  (let [result-code (.minvalid tair namespace keys)]
    (clojurify-result-code result-code)))

(defn mdelete
  [tair namespace keys]
  (let [result-code (.mdelete tair namespace keys)]
    (clojurify-result-code result-code)))

(defn get-range
  [tair namespace prefix key-start key-end offset limit]
  (let [ret (.getRange tair namespace prefix key-start key-end offset limit)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              [])
        ret (map #(vector (.getKey %) (.getValue %)) ret)]
    ret))

(defn get-range-only-key
  [tair namespace prefix key-start key-end offset limit]
  (let [ret (.getRange tair namespace prefix key-start key-end offset limit)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              [])
        ret (map #(.getKey %) ret)]
    ret))

(defn get-range-only-value
  [tair namespace prefix key-start key-end offset limit]
  (let [ret (.getRange tair namespace prefix key-start key-end offset limit)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              [])
        ret (map #(.getValue %) ret)]
    ret))

(defn incr
  [tair namespace key value default-value expire-time]
  (let [ret (.incr tair namespace key value default-value expire-time)]
    ret))

(defn decr
  [tair namespace key value default-value expire-time]
  (let [ret (.decr tair namespace key value default-value expire-time)]
    ret))

(defn set-count
  ([tair namespace key count]
     (set-count tair namespace key count (int 0) (int 0)))
  ([tair namespace key count version expire-time]
     (let [result-code (.setCount tair namespace key count version expire-time)]
       (clojurify-result-code result-code))))

(defn add-items
  [tair namespace key items max-count version expire-time]
  (let [result-code (.addItems tair namespace key items max-count version expire-time)]
    (clojurify-result-code result-code)))

(defn get-items
  [tair namespace key offset count]
  (let [ret (.getItems tair namespace key offset count)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (-> ret .getValue .getValue)
              nil)]
    ret))

(defn remove-items
  [tair namespace key offset count]
  (let [result-code (.removeItems tair namespace key offset count)]
    (clojurify-result-code result-code)))

(defn get-and-remove
  [tair namespace key offset count]
  (let [ret (.getAndRemove tair namespace key offset count)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (-> ret .getValue .getValue)
              nil)]
    ret))

(defn get-item-count
  [tair namespace key]
  (let [ret (.getItemCount tair namespace key)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              0)]
    ret))

(defn lock
  [tair namespace key]
  (let [result-code (.lock tair namespace key)]
    (clojurify-result-code result-code)))

(defn unlock
  [tair namespace key]
  (let [result-code (.unlock tair namespace key)]
    (clojurify-result-code result-code)))

(defn mlock
  ([tair namespace keys]
     (mlock tair namespace keys false))
  ([tair namespace keys need-fail-keys-map?]
     (let [[ret fail-keys-map] (if need-fail-keys-map?
                                 (let [fail-keys-map (HashMap.)
                                       temp-ret (.mlock tair namespace keys fail-keys-map)
                                       fail-keys-map (into {} (map #(vector (.getKey %)
                                                                            (clojurify-result-code (.getValue %)))
                                                                   fail-keys-map))]
                                   [temp-ret fail-keys-map])
                                 [(.mlock tair namespace keys) {}])
           result-code (clojurify-result-code (.getRc ret))
           data (vec (.getValue ret))
           ret {:rc result-code :data data :fail-keys-map fail-keys-map}]
       ret)))

(defn munlock
  ([tair namespace keys]
     (munlock tair namespace keys false))
  ([tair namespace keys need-fail-keys-map?]
     (let [[ret fail-keys-map] (if need-fail-keys-map?
                                 (let [fail-keys-map (HashMap.)
                                       temp-ret (.munlock tair namespace keys fail-keys-map)
                                       fail-keys-map (into {} (map #(vector (.getKey %)
                                                                            (clojurify-result-code (.getValue %)))
                                                                   fail-keys-map))]
                                   [temp-ret fail-keys-map])
                                 [(.munlock tair namespace keys) {}])
           result-code (clojurify-result-code (.getRc ret))
           data (vec (.getValue ret))
           ret {:rc result-code :data data :fail-keys-map fail-keys-map}]
       ret)))

(defn get-stat
  [tair qtype group-name server-id]
  (let [ret (.getStat tair qtype group-name server-id)]
    (into {} ret)))

(defn get-version [tair]
  (.getVersion tair))

(defn get-max-fail-count [tair]
  (.getMaxFailCount tair))

(defn set-max-fail-count [tair cnt]
  (.setMaxFailCount tair cnt))

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

