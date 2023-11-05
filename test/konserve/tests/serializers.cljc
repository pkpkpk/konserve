(ns konserve.tests.serializers
  (:require [clojure.core.async :as a :refer [<! go]]
            [clojure.test :refer [is testing]]
            [fress.api :as fress]
            [konserve.core :as k]
            [konserve.serializers :refer [fressian-serializer]]
            [incognito.base #?@(:cljs [:refer [IncognitoTaggedLiteral]])])
  #?(:clj
     (:import [org.fressian.handlers WriteHandler ReadHandler]
              [incognito.base IncognitoTaggedLiteral]
              (java.util Date))))

(deftype MyType [a b]
  #?@(:clj
     (Object
      (equals [this other]
              (and (instance? MyType other)
                   (= a (.a ^MyType other))
                   (= b (.b ^MyType other))))
      (hashCode [this] (hash [a b])))
     :cljs
     (IEquiv
      (-equiv [this other]
              (and (instance? MyType other)
                   (= a (.-a ^MyType other))
                   (= b (.-b ^MyType other)))))))

(defrecord MyRecord [a b])

(def custom-read-handlers
  {"my-type"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
          (MyType. (fress/read-object rdr)
                   (fress/read-object rdr))))
      :cljs
      (fn [rdr _ _]
        (MyType. (fress.api/read-object rdr)
                 (fress.api/read-object rdr))))
   "custom-tag"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
              (Date. ^long (.readObject rdr))))
      :cljs
      (fn [rdr _ _]
        (doto (js/Date.)
          (.setTime (fress/read-object rdr)))))
   "my-record"
   #?(:clj
      (reify ReadHandler
        (read [_ rdr _ _]
              (MyRecord. (fress/read-object rdr)
                         (fress/read-object rdr))))
      :cljs
      (fn [rdr _ _]
        (MyRecord. (fress/read-object rdr)
                   (fress/read-object rdr))))})

(def custom-write-handlers
  {#?(:clj Date :cljs js/Date)
   {"custom-tag"
    #?(:clj
       (reify WriteHandler
         (write [_ writer instant]
                (fress/write-tag    writer "custom-tag" 1)
                (fress/write-object writer (.getTime ^Date instant))))
       :cljs
       (fn [wrt date]
         (fress/write-tag wrt "custom-tag" 1)
         (fress/write-object wrt (.getTime date))))}
   MyType {"my-type"
           #?(:clj
              (reify WriteHandler
                (write [_ writer o]
                       (fress/write-tag writer "my-type" 2)
                       (fress/write-object writer ^MyType (.-a o))
                       (fress/write-object writer ^MyType (.-b o))))
              :cljs
              (fn [writer o]
                (fress/write-tag writer "my-type" 2)
                (fress/write-object writer (.-a o))
                (fress/write-object writer (.-b o))))}
   MyRecord {"my-record"
             #?(:clj
                (reify WriteHandler
                  (write [_ writer o]
                         (fress/write-tag writer "my-record" 2)
                         (fress/write-object writer (.-a o))
                         (fress/write-object writer (.-b o))))
                :cljs
                (fn [writer o]
                  (fress/write-tag writer "my-record" 2)
                  (fress/write-object writer (.-a o))
                  (fress/write-object writer (.-b o))))}})

(defn test-fressian-serializers-async
  [store-name connect-store delete-store-async]
  (go
   (and
    (testing ":serializers arg to connect-store"
      (let [serializers {:FressianSerializer (fressian-serializer custom-read-handlers
                                                                  custom-write-handlers)}
            _(assert (nil? (<! (delete-store-async "/tmp/serializers-test"))))
            store (<! (connect-store store-name :serializers serializers))
            d #?(:clj (Date.) :cljs (js/Date.))
            my-type (MyType. "a" "b")
            my-record (map->MyRecord {:a 0 :b 1})]
        (and
         (is (nil? (<! (k/get-in store [:foo]))))
         (is [nil 42] (<! (k/assoc-in store [:foo] 42)))
         (is (= 42 (<! (k/get-in store [:foo]))))
         (is (= [nil d] (<! (k/assoc-in store [:foo] d))))
         ;; TODO verify tag
         (is (= d (<! (k/get-in store [:foo]))))
         (is (= [nil my-type] (<! (k/assoc-in store [:foo] my-type))))
         (is (= my-type (<! (k/get-in store [:foo]))))
         (is (= [nil my-record] (<! (k/assoc-in store [:foo] my-record))))
         (is (= my-record (<! (k/get-in store [:foo])))))))
    (testing "records are intercepted by incognito when write-handler isn't specified"
      (let [_(assert (nil? (<! (delete-store-async "/tmp/serializers-test"))))
            store (<! (connect-store store-name))
            my-record (map->MyRecord {:a 0 :b 1})]
        (and
         (is (= [nil my-record] (<! (k/assoc-in store [:bar] my-record))))
         (is (instance? IncognitoTaggedLiteral (<! (k/get-in store [:bar])))))))
    (testing ":read-handlers arg to connect-store let's us recover records"
      (let [_(assert (nil? (<! (delete-store-async "/tmp/serializers-test"))))
            read-handlers {'konserve.tests.serializers.MyRecord map->MyRecord}
            store (<! (connect-store store-name
                                     :read-handlers (atom read-handlers)))
            my-record (map->MyRecord {:a 0 :b 1})]
        (and
         (is (nil? (<! (k/get-in store [:foo]))))
         (is (= [nil my-record] (<! (k/assoc-in store [:foo] my-record))))
         (is (= my-record (<! (k/get-in store [:foo]))))))))))

#_
(deftest cbor-serializer-test
  (testing "Test CBOR serializer functionality."
    (let [folder "/tmp/konserve-fs-cbor-test"
          _      (delete-store folder)
          store  (<!! (connect-fs-store folder :default-serializer :CBORSerializer))]
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/assoc-in store [:foo] (Date.)))
      (is (= (type (<!! (k/get-in store [:foo])))
             java.time.Instant))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (delete-store folder))))