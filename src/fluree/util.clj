(ns fluree.util
  (:require [clojure.walk :as walk]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (java.util Hashtable$Entry)
           (java.lang.management ManagementFactory)))

(defn filter-vals
  "Filters map k/v pairs dropping any where predicate applied to value is false."
  [pred m]
  (reduce-kv (fn [m k v] (if (pred v) (assoc m k v) m)) {} m))

(defn without-nils
  "Remove all keys from a map that have nil or empty collection values."
  [m]
  (filter-vals #(if (coll? %) (not-empty %) (some? %)) m))

(defn keyword->str
  "Converts a keyword to string. Can safely be called on a
  string which will return itself."
  [k]
  (cond
    (keyword? k) (subs (str k) 1)
    (string? k) k
    :else (throw (ex-info (str "Cannot convert type " (type k) " to string: " (pr-str k))
                          {:status 500 :error :db/unexpected-error}))))

(defn read-properties-file
  "Reads properties file at file-name, if it doesn't exist returns nil.
  By default, this is fluree_sample.properties unless otherwise specified in a flag"
  [file-name]
  (let [file (io/file file-name)]
    (when (.exists file)
      (with-open [^java.io.Reader reader (io/reader file)]
        (let [props (java.util.Properties.)]
          (.load props reader)
          (->> (for [prop props]
                 (let [k (.getKey ^Hashtable$Entry prop)
                       v (.getValue ^Hashtable$Entry prop)]
                   (if (= "" v)
                     nil
                     [(keyword k) (if (= "" v) nil v)])))
               (into {})
               (without-nils)))))))

(defn jvm-arguments
  []
  (let [jvm-name   (System/getProperty "java.vm.name")
        input-args (-> (ManagementFactory/getRuntimeMXBean) (.getInputArguments))]
    {:jvm jvm-name :input input-args}))

(defn jvm-args->map
  [input]
  (-> (reduce (fn [acc setting]
                (if (str/starts-with? setting "-D")
                  (let [[k v] (-> setting
                                  (str/replace-first "-D" "")
                                  (str/split #"="))]
                    (assoc acc k v))
                  acc)) {} input) walk/keywordize-keys))

(defn ledger-name?
  [lid]
  (= 2 (count (str/split (str lid) #"/"))))

(defn get-ledger-name
  [args]
  (condp = (count args) 2 (map str args)
                        1 (let [ledger (str/split (-> args first str) #"/")]
                            (if (= 2 (count ledger))
                              ledger
                              (throw (ex-info
                                       (str "Invalid ledger-id provided. Expected either `network/ledger` or `network ledger`. Provided: "
                                            (str/join " " args))
                                       {:status 400
                                        :error  :db/invalid-command}))))
                        :else
                        (throw (ex-info (str "Invalid ledger-id provided. Expected either `network/ledger` or `network ledger`. Provided: "
                                             (str/join " " args))
                                        {:status 400
                                         :error  :db/invalid-command}))))
(defn parse-nw-lid-start-end-args
  [args]
  (let [[nw lid] (if (ledger-name? (first args))
                   (get-ledger-name [(first args)])
                   (get-ledger-name [(first args) (second args)]))
        second-to-last-arg (nth args (- (count args) 2))
        [startBlock endBlock] (if (int? second-to-last-arg)
                                [second-to-last-arg (last args)]
                                [(last args) nil])]
    [nw lid startBlock endBlock]))


(defn get-input
  [varname value]
  (do (print (str "New " varname " (enter to keep " value "): "))
      (flush)
      (read-line)))

(defn format-path
  [config-path]
  (cond-> config-path
          (not (or (str/starts-with? config-path "./") (str/starts-with? config-path "/"))) (#(str "./" %))
          (not (str/ends-with? config-path "/")) (str "/")))