(ns tair.core
  (:refer-clojure :exclude [get])
  (:import [com.taobao.tair TairManager ResultCode CallMode]
           [com.taobao.tair Result DataEntry TairCallback]
           [com.taobao.tair.packet BasePacket]
           [com.taobao.tair.impl.mc MultiClusterTairManager]
           [com.taobao.tair.etc KeyValuePack CounterPack]
           [com.alibaba.fastjson JSON]
           [java.net URL]
           [java.util Map HashMap HashMap$Entry List])
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

(defn- get-data-entry [^Result result]
  (if (and (not (nil? result))
           (not (nil? (.getValue ^Result result)))
           (not (nil? (.getValue ^DataEntry (.getValue result)))))
    (pretify-result (.getValue ^DataEntry (.getValue result)))
    nil))

(defn- get-data-entry-list [^Result ret]
  (if (and (not (nil? ret))
           (not (nil? (-> ret .getValue))))
    (map #(vector (.getKey ^DataEntry %) (.getValue ^DataEntry %)) (.getValue ret))
    ()))

(defn- get-object->resultcode-map [^Result ret]
  (let [ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              {})
        ret (into {} (map #(vector (first %) (clojurify-result-code (second %))) ret))]
    ret))

(defn- get-object->result_dataentry-map [^Result ret]
  (let [ret (if (and (not (nil? ret))
                     (not (nil? (-> ret .getValue))))
              (into {} (map #(vector (first %) (.getValue ^DataEntry (.getValue ^Result (second %)))) (.getValue ret)))
              {})]
    ret))

(defn- get-object->result_integer-map [^Result ret]
  (let [result (if (and (not (nil? ret))
                        (not (nil? (-> ret .getValue))))
                 (into {} (map #(vector (first %) (.getValue ^Result (second %))) (.getValue ret)))
                 {})]
    result))

(defn get
  "Query the value of the specified key from the specified namespace."
  [^TairManager tair namespace key]
  (let [^Result ret (.get tair namespace key)]
    (get-data-entry ret)))

(defn mget
  "Batch get data from tair

  tair: the tair manager
  namespace: the namespace where the data is in
  keys: a collection of keys to get"
  [^TairManager tair namespace keys]
  (let [^Result ret (.mget tair namespace (vec keys))
        ret (get-data-entry-list ret)]
    ret))

(defn put
  "Put the key value pair into the specified namespace with the specified expiretime.

  If 0 is specified for the expiretime, it means it will not expire
  If fill-cache? is true, then it will also insert the data into cache (only
  applies when server-side support cache."
  ([^TairManager tair namespace key value]
     (put tair namespace key value 0))
  ([^TairManager tair namespace key value version]
     (put tair namespace key value version 0))
  ([^TairManager tair namespace key value version expire-time]
     (put tair namespace key value version 0 true))
  ([^TairManager tair namespace key value version expire-time fill-cache?]
     (let [result-code (.put tair namespace key value version expire-time fill-cache?)
           result-code (clojurify-result-code result-code)]
       result-code)))

(defn put-async
  [^TairManager tair namespace key value version expire-time fill-cache?
   callback-base-packet-fn callback-exception-fn]
  (let [tair-callback (reify TairCallback
                        (^void callback [this ^BasePacket packet]
                          (if callback-base-packet-fn
                            (callback-base-packet-fn packet)
                            nil))
                        (^void callback [this ^Exception e]
                          (if callback-exception-fn
                            (callback-exception-fn e)
                            nil)))
        result-code (.putAsync tair namespace key value version expire-time
                               fill-cache? tair-callback)]
    (clojurify-result-code result-code)))

(defn delete
  "Delete the specified key from tair"
  [^TairManager tair namespace key]
  (let [result-code (.delete tair namespace key)
        result-code (clojurify-result-code result-code)]
    result-code))

(defn invalid
  "
  callmode can be one of [:sync :async]"
  ([^TairManager tair namespace key]
     (invalid tair namespace key :sync))
  ([^TairManager tair namespace key callmode]
     (let [callmode (if (= :sync callmode)
                      CallMode/SYNC
                      CallMode/ASYNC)
           result-code (.invalid tair namespace key callmode)]
       (clojurify-result-code result-code))))

(defn hide
  [^TairManager tair namespace key]
  (let [result-code (.hide tair namespace key)]
    (clojurify-result-code result-code)))

(defn hide-by-proxy
  ([^TairManager tair namespace key]
     (hide-by-proxy tair namespace key :sync))
  ([^TairManager tair namespace key callmode]
     (let [callmode (if (= :sync callmode)
                      CallMode/SYNC
                      CallMode/ASYNC)
           result-code (.hideByProxy tair namespace key callmode)]
       (clojurify-result-code result-code))))

(defn get-hidden
  [^TairManager tair namespace key]
  (let [^Result obj (.getHidden tair namespace key)
        obj (get-data-entry obj)]
    obj))

(defn prefix-put
  ([^TairManager tair namespace pkey skey value]
     (prefix-put tair namespace pkey skey value 0))
  ([^TairManager tair namespace pkey skey value version]
     (prefix-put tair namespace pkey skey value version 0))
  ([^TairManager tair namespace pkey skey value version expire-time]
     (let [result-code (.prefixPut tair namespace pkey skey value version expire-time)]
       (clojurify-result-code result-code))))

(defn prefix-puts
  [^TairManager tair namespace pkey skey-value-packs]
  (let [skey-value-packs (map (fn [key-value-pack]
                                (case (count key-value-pack)
                                  2 (KeyValuePack. (first key-value-pack) (second key-value-pack))
                                  3 (KeyValuePack. (first key-value-pack) (second key-value-pack) (nth key-value-pack 2))
                                  4 (KeyValuePack. (first key-value-pack) (second key-value-pack) (nth key-value-pack 2) (nth key-value-pack 3)))) skey-value-packs)
        result (.prefixPuts tair namespace pkey skey-value-packs)
        result (get-object->resultcode-map result)]
    result))

(defn prefix-delete
  [^TairManager tair namespace pkey skey]
  (let [result-code (.prefixDelete tair namespace pkey skey)]
    (clojurify-result-code result-code)))

(defn prefix-deletes
  [^TairManager tair namespace pkey skeys]
  (let [result (.prefixDeletes tair namespace pkey skeys)
        result (get-object->resultcode-map result)]
    result))

(defn prefix-get
  [^TairManager tair namespace pkey skey]
  (let [^Result obj (.prefixGet tair namespace pkey skey)
        obj (get-data-entry obj)]
    obj))

(defn prefix-gets
  [^TairManager tair namespace pkey skeys]
  (let [^Result ret (.prefixGets tair namespace pkey (vec skeys))
        ret (get-object->result_dataentry-map ret)
        ]
    ret))

(defn prefix-incr
  [^TairManager tair namespace pkey skey value default-value expire-time]
  (let [obj (.prefixIncr tair namespace pkey skey value default-value expire-time)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue))))
              (-> obj .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-incrs
  [^TairManager tair namespace pkey skey-packs]
  (let [skey-packs (map (fn [key-pack]
                                (case (count key-pack)
                                  3 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2))
                                  4 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2) (nth key-pack 3)))) skey-packs)
        ^Result result (.prefixIncrs tair namespace pkey skey-packs)
        result (get-object->result_integer-map result)]
    result))

(defn prefix-decr
  [^TairManager tair namespace pkey skey value default-value expire-time]
  (let [obj (.prefixDecr tair namespace pkey skey value default-value expire-time)
        obj (if (and (not (nil? obj))
                     (not (nil? (-> obj .getValue))))
              (-> obj .getValue
                  pretify-result)
              nil)]
    obj))

(defn prefix-decrs
  [^TairManager tair namespace pkey skey-packs]
  (let [skey-packs (map (fn [key-pack]
                                (case (count key-pack)
                                  3 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2))
                                  4 (CounterPack. (first key-pack) (second key-pack) (nth key-pack 2) (nth key-pack 3)))) skey-packs)
        ^Result result (.prefixDecrs tair namespace pkey skey-packs)
        result (get-object->result_integer-map result)]
    result))

(defn prefix-set-count
  ([^TairManager tair namespace pkey skey count]
     (prefix-set-count tair namespace pkey skey count 0 0))
  ([^TairManager tair namespace pkey skey count version expire-time]
     (let [result-code (.prefixSetCount tair namespace pkey skey count version expire-time)]
       (clojurify-result-code result-code))))

(defn prefix-hide
  [^TairManager tair namespace pkey skey]
  (let [result-code (.prefixHide tair namespace pkey skey)]
    (clojurify-result-code result-code)))

(defn prefix-hides
  [^TairManager tair namespace pkey skeys]
  (let [ret (.prefixHides tair namespace pkey skeys)
        ret (get-object->resultcode-map ret)]
    ret))

(defn prefix-get-hidden
  [^TairManager tair namespace pkey skey]
  (let [^Result ret (.prefixGetHidden tair namespace pkey skey)
        ret (get-data-entry ret)]
    ret))

(defn prefix-get-hiddens
  [^TairManager tair namespace pkey skeys]
  (let [^Result ret (.prefixGetHiddens tair namespace pkey (vec skeys))
        ret (get-object->result_dataentry-map ret)]
    ret))

(defn prefix-invalid
  [^TairManager tair namespace pkey skey callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        result-code (.prefixInvalid tair namespace pkey skey callmode)]
    (clojurify-result-code result-code)))

(defn prefix-invalids
  [^TairManager tair namespace pkey skeys callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        ret (.prefixInvalids tair namespace pkey skeys callmode)
        ret (get-object->resultcode-map ret)]
    ret))

(defn prefix-hide-by-proxy
  [^TairManager tair namespace pkey skey callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        result-code (.prefixHideByProxy tair namespace pkey skey callmode)]
    (clojurify-result-code result-code)))

(defn prefix-hides-by-proxy
  [^TairManager tair namespace pkey skeys callmode]
  (let [callmode (if (= callmode :sync)
                   CallMode/SYNC
                   CallMode/ASYNC)
        ret (.prefixHidesByProxy tair namespace pkey skeys callmode)
        ret (get-object->resultcode-map ret)]
    ret))

(defn mprefix-get-hiddens
  [^TairManager tair namespace p-s-keys]
  (let [^Result ret (.mprefixGetHiddens tair namespace p-s-keys)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              {})
        ret (into {} (map (fn [[pkey values]]
                            [pkey
                             (into {} (map #(vector (first %)
                                                    (.getValue ^DataEntry (.getValue ^Result (second %))))
                                           values))])
                          ret))]
    ret))

(defn minvalid
  [^TairManager tair namespace keys]
  (let [result-code (.minvalid tair namespace keys)]
    (clojurify-result-code result-code)))

(defn mdelete
  [^TairManager tair namespace keys]
  (let [result-code (.mdelete tair namespace keys)]
    (clojurify-result-code result-code)))

(defn get-range
  [^TairManager tair namespace prefix key-start key-end offset limit]
  (let [^Result ret (.getRange tair namespace prefix key-start key-end offset limit)
        ret (get-data-entry-list ret)]
    ret))

(defn get-range-only-key
  [^TairManager tair namespace prefix key-start key-end offset limit]
  (let [^Result ret (.getRangeOnlyKey tair namespace prefix key-start key-end offset limit)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              [])
        ret (map #(.getKey ^DataEntry %) ret)]
    ret))

(defn get-range-only-value
  [^TairManager tair namespace prefix key-start key-end offset limit]
  (let [^Result ret (.getRangeOnlyValue tair namespace prefix key-start key-end offset limit)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ^Result ret)
              [])
        ret (map #(.getValue ^DataEntry %) ret)]
    ret))

(defn incr
  [^TairManager tair namespace key value default-value expire-time]
  (let [^Result ret (.incr tair namespace key value default-value expire-time)]
    (.getValue ret)))

(defn decr
  [^TairManager tair namespace key value default-value expire-time]
  (let [^Result ret (.decr tair namespace key value default-value expire-time)]
    (.getValue ret)))

(defn set-count
  ([^TairManager tair namespace key count]
     (set-count tair namespace key count (int 0) (int 0)))
  ([^TairManager tair namespace key count version expire-time]
     (let [result-code (.setCount tair namespace key count version expire-time)]
       (clojurify-result-code result-code))))

(defn add-items
  [^TairManager tair namespace key items max-count version expire-time]
  (let [result-code (.addItems tair namespace key items max-count version expire-time)]
    (clojurify-result-code result-code)))

(defn get-items
  [^TairManager tair namespace key offset count]
  (let [^Result ret (.getItems tair namespace key offset count)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ^DataEntry (.getValue ret))
              nil)]
    ret))

(defn remove-items
  [^TairManager tair namespace key offset count]
  (let [result-code (.removeItems tair namespace key offset count)]
    (clojurify-result-code result-code)))

(defn get-and-remove
  [^TairManager tair namespace key offset count]
  (let [^Result ret (.getAndRemove tair namespace key offset count)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ^DataEntry (.getValue ret))
              nil)]
    ret))

(defn get-item-count
  [^TairManager tair namespace key]
  (let [ret (.getItemCount tair namespace key)
        ret (if (and (not (nil? ret))
                     (not (nil? (.getValue ret))))
              (.getValue ret)
              0)]
    ret))

(defn lock
  [^TairManager tair namespace key]
  (let [result-code (.lock tair namespace key)]
    (clojurify-result-code result-code)))

(defn unlock
  [^TairManager tair namespace key]
  (let [result-code (.unlock tair namespace key)]
    (clojurify-result-code result-code)))

(defn mlock
  ([^TairManager tair namespace keys]
     (mlock tair namespace keys false))
  ([^TairManager tair namespace keys need-fail-keys-map?]
     (let [[^Result ret fail-keys-map] (if need-fail-keys-map?
                                 (let [fail-keys-map (HashMap.)
                                       temp-ret (.mlock tair namespace keys fail-keys-map)
                                       fail-keys-map (into {} (map #(vector (.getKey ^HashMap$Entry %)
                                                                            (clojurify-result-code (.getValue ^HashMap$Entry %)))
                                                                   fail-keys-map))]
                                   [temp-ret fail-keys-map])
                                 [(.mlock tair namespace keys) {}])
           result-code (clojurify-result-code (.getRc ret))
           data (vec (.getValue ret))
           ret {:rc result-code :data data :fail-keys-map fail-keys-map}]
       ret)))

(defn munlock
  ([^TairManager tair namespace keys]
     (munlock tair namespace keys false))
  ([^TairManager tair namespace keys need-fail-keys-map?]
     (let [[^Result ret fail-keys-map] (if need-fail-keys-map?
                                 (let [fail-keys-map (HashMap.)
                                       temp-ret (.munlock tair namespace keys fail-keys-map)
                                       fail-keys-map (into {} (map #(vector (.getKey ^HashMap$Entry %)
                                                                            (clojurify-result-code (.getValue ^HashMap$Entry %)))
                                                                   fail-keys-map))]
                                   [temp-ret fail-keys-map])
                                 [(.munlock tair namespace keys) {}])
           result-code (clojurify-result-code (.getRc ret))
           data (vec (.getValue ^Result ret))
           ret {:rc result-code :data data :fail-keys-map fail-keys-map}]
       ret)))

(defn get-stat
  [^TairManager tair qtype group-name server-id]
  (let [ret (.getStat tair qtype group-name server-id)]
    (into {} ret)))

(defn get-version [^TairManager tair]
  (.getVersion tair))

(defn get-max-fail-count [^TairManager tair]
  (.getMaxFailCount tair))

(defn set-max-fail-count [^TairManager tair cnt]
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
