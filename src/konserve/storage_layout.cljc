(ns konserve.storage-layout
  "One of these protocols must be implemented by each store to provide low level
  access depending on the low-level storage layout chosen. Stores can support
  multiple layouts."
  (:require [konserve.serializers :refer [byte->key]])
  #?(:clj (:import [java.nio ByteBuffer])))

(def ^:const header-size 20)

(defn create-header
  "Return Byte Array with following content
     1th Byte = Storage layout used
     2th Byte = Serializer Type
     3th Byte = Compressor Type
     4th Byte = Encryptor Type
   5-8th Byte = Meta-Size
  9th-20th Byte are spare"
  [storage-layout serializer compressor encryptor meta]
  #?(:clj
     (let [env-array        (byte-array [storage-layout serializer compressor encryptor])
           return-buffer    (ByteBuffer/allocate header-size)
           _                (.put return-buffer env-array)
           _                (.putInt return-buffer 4 meta)
           return-array     (.array return-buffer)]
       (.clear return-buffer)
       return-array)
     :cljs (throw (ex-info "Not supported yet." {}))))

(defn parse-header
  "Inverse function to create-header."
  [header-bytes]
  #?(:clj
     (let [bb (ByteBuffer/allocate header-size)]
       (.put bb ^bytes header-bytes)
       [(.get bb 0) (.get bb 1) (.get bb 2) (.get bb 3) (.getInt bb 4)])
     :cljs
     (throw (ex-info "Not supported yet." {}))))

(def ^:const linear-layout-id 1)

(defprotocol PLinearLayout
  ;; Location 1: [4-header-bytes 4-bytes-for-meta-size serialized-meta serialized-data]
  (-get-raw [store key opts])
  (-put-raw [store key blob opts]))

(def ^:const split-layout-id 1)

(defprotocol PSplitLayout
  ;; Location 1: [4-header-bytes serialized-meta]
  ;; Location 2: [4-header-bytes serialized-data]
  (-get-raw-meta [store key opts])
  (-put-raw-meta [store key blob opts])
  (-get-raw-value [store key opts])
  (-put-raw-value [store key blob opts]))
