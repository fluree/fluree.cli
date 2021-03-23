(defproject fluree.cli "0.2.0-SNAPSHOT"
  :description "The Fluree Command Line Interface (CLI) is a terminal-based tool that allows users to read and verify Fluree ledger files and consensus logs."
  :url "https://github.com/fluree/fluree.cli"
  :license "SEE LICENSE IN LICENSE"
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.clojure/data.avl "0.1.0"]
                 [cheshire "5.10.0"]
                 [com.fluree/raft "1.0.0-beta1"]
                 [com.fluree/crypto "0.3.5" :exclusions [org.clojure/clojurescript]]
                 [cli4clj "1.7.6" :exclusions [org.clojure/core.async]]
                 [clj-figlet "0.1.1"]
                 [com.damballa/abracad "0.4.13"]]
  :main fluree.core
  :aot :all)
