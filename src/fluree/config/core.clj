(ns fluree.config.core
  (:require [fluree.util :as util]
            [clojure.string :as str]))

(defn config-item
  [varname value]
  (let [value*  (util/get-input varname value)
        new-val (if (or (nil? value*) (= "" (str/trim value*)))
                  value
                  value*)
        _       (print new-val)
        _       (println)]
    new-val))

(defn config-api
  [config-atom]
  (fn [& args]
    (let [config-type (first args)
          config-type (if (nil? config-type)
                        nil (-> config-type str str/lower-case))]
      (cond
        (or (nil? config-type) (= "get" config-type))
        (do (println "Current Fluree config: " @config-atom ".")
            (println "To change, use the command `config set`"))

        (= "set" config-type)
        (let [{:keys [port data-dir log-dir this-server]} @config-atom
              _           (println "Current Fluree config: " @config-atom)
              port        (config-item "port" port)
              data-dir    (config-item "data directory" data-dir)
              log-dir     (config-item "log directory" log-dir)
              this-server (config-item "server" this-server)]
          (swap! config-atom merge {:port        port
                                    :data-dir    data-dir
                                    :log-dir     log-dir
                                    :this-server this-server}))))))
