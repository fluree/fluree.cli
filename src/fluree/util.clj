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
  (let [file        (io/file file-name)
        valid-file? (and (.exists file) (.isFile file))]
    (if valid-file?
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
               (without-nils))))
      (do
        (println "** WARNING: Supplied properties file is not valid, no properties loaded.")
        (println "            -> Use `config set` to manually set needed properties,")
        (println "               or `quit` and try again with a valid properties file.")))))

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

(defn parse-opts
  "Returns a two-tuple of options in a map and a sequence of args.
  options start with '--' and optionally have a value, i.e.:
        --output-file=temp/file.json --verbose
  would get translated into a map like:
  {:verbose     nil
   :output-file 'temp/file.json'}
   All option values will be strings, or nil - the receiving function will need
   to coerce types if needed.

  Returned args are untouched with the exception of extracting any options, if applicable."
  [args]
  (let [[opts args*] (->> args
                          (map str)                         ;; input type of some args is symbol
                          ((juxt filter remove) #(str/starts-with? % "--")))
        opts-map (reduce (fn [acc opt]
                           (let [[k v] (str/split opt #"=")
                                 k* (keyword (str/replace k #"^--" ""))
                                 v* (cond
                                      ;; if :output-file, ensure file name has '.json' appended
                                      (= :output-file k*)
                                      (if (str/ends-with? v ".json") v (str v ".json"))

                                      ;; if an integer, try to parse
                                      :else
                                      (try (Integer/parseInt v) ;; parse to integer if possible
                                           (catch Exception _ v)))]
                             (assoc acc k* v*))) {} opts)
        ;; parse any args possible into integers
        args**   (map #(try (Integer/parseInt %) (catch Exception _ %)) args*)]
    [opts-map args**]))

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

(defn parse-nw-lid-start-end-args
  [data-dir args]
  (let [[opts args*] (parse-opts args)
        data-dir* (some-> (or (:data-dir opts) data-dir)
                          (format-path))
        [ledger start-block end-block-or-meta] args*]
    (when-not (ledger-name? ledger)
      (throw (ex-info
               (str "Invalid ledger provided. Expected `ledger-network/ledger-id`. Provided: " ledger)
               {:status 400
                :error  :db/invalid-arguments})))
    (when-not data-dir*
      (throw (ex-info
               "No ledger data-dir provided. Either use optional `--data-dir=/path/to/ledger/files` or command `config set` to set default dir."
               {:status 400
                :error  :db/invalid-arguments})))
    [data-dir* ledger start-block end-block-or-meta opts]))

(defn parse-ledger-compare-args
  [args]
  (let [[opts args*] (parse-opts args)
        [ledger & data-dirs] args*
        data-dirs* (map format-path data-dirs)]
    (when (= 1 (count data-dirs*))
      (throw (ex-info "data-dirs argument must be a list of data directories separated by commas with no spaces."
                      {:status :400
                       :error  :db/invalid-arguments})))
    (when-not (ledger-name? ledger)
      (throw (ex-info
               (str "Invalid ledger provided. Expected `ledger-network/ledger-id`. Provided: "
                    (str/join " " args))
               {:status 400
                :error  :db/invalid-arguments})))
    [ledger data-dirs* opts]))

;; adapted from https://gist.github.com/jkk/345785
(defn glob->regex [s]
  "Takes a glob-format string and returns a regex."
  (let [stream (java.io.StringReader. s)]
    (loop [i           (.read stream)
           re          ""
           curly-depth 0]
      (if (= i -1)
        (re-pattern re)
        (let [c (char i)
              j (.read stream)]
          (cond
            (= c \\) (recur (.read stream) (str re c (char j)) curly-depth)
            (= c \*) (recur j (str re ".*") curly-depth)
            (= c \?) (recur j (str re \.) curly-depth)
            (= c \{) (recur j (str re \() (inc curly-depth))
            (= c \}) (recur j (str re \)) (dec curly-depth))
            (and (= c \,) (< 0 curly-depth)) (recur j (str re \|) curly-depth)
            (#{\. \( \) \| \+ \^ \$ \@ \%} c) (recur j (str re \\ c) curly-depth)
            :else (recur j (str re c) curly-depth)))))))

(defn has-glob?
  "Returns true if the string has either a '*' or '?' located in it."
  [s]
  (re-matches #"^.*[\*\?].*$" s))
