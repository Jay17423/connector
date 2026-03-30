(ns connector.gdrive
  "Downloads CSV from Google Drive and cleans it using streaming."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.net URI]
           [java.net.http HttpClient
            HttpClient$Redirect
            HttpRequest
            HttpResponse$BodyHandlers]
           [java.time Duration]
           [java.util UUID]))

(def http-client
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 30))
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn extract-file-id
  "Extracts file-id from Google Drive link."
  [url]
  (or (some (fn [pat]
              (when-let [m (re-find pat url)]
                (second m)))
            [#"/file/d/([a-zA-Z0-9_-]+)"
             #"[?&]id=([a-zA-Z0-9_-]+)"
             #"/open\\?id=([a-zA-Z0-9_-]+)"])
      (throw (ex-info "Cannot extract Drive file ID from URL"
                      {:url url}))))

(defn build-download-url
  "Builds Drive download URL using revision-id if present."
  [file-id revision-id]
  (if revision-id
    (str "https://www.googleapis.com/drive/v3/files/"
         file-id
         "/revisions/"
         revision-id
         "?alt=media")
    (str "https://www.googleapis.com/drive/v3/files/"
         file-id
         "?alt=media")))

(defn build-request
  "Creates authorized HTTP request."
  [url-str token]
  (-> (HttpRequest/newBuilder)
      (.uri (URI/create url-str))
      (.timeout (Duration/ofSeconds 120))
      (.header "Authorization" (str "Bearer " token))
      (.GET)
      (.build)))

(defn stream-clean-to-file!
  "Streams CSV, removes BOM + empty rows."
  [input-stream dest-path {:keys [delimiter quote-char]
                           :or {delimiter "," quote-char \"}}]
  (let [sep (first delimiter)
        qc  (if (char? quote-char)
              quote-char
              (first (str quote-char)))]
    (with-open [rdr (io/reader input-stream)
                wtr (io/writer dest-path)]
      (let [[hdr & rows]
            (csv/read-csv rdr :separator sep :quote qc)
            header
            (when hdr
              (assoc hdr 0
                     (let [v (first hdr)]
                       (if (and v (= (first v) \uFEFF))
                         (subs v 1)
                         v))))
            rows
            (remove (fn [row] (every?
                               (fn [col]
                                 (str/blank? (or col "")))
                               row))
                    rows)]
        (csv/write-csv
         wtr
         (cons header rows)
         :separator sep
         :quote qc)))))

(defn fetch-and-clean!
  "Downloads CSV from Drive and returns cleaned temp path."
  [{:keys [link token revision-id]} options]

  (let [file-id (extract-file-id link)
        url (build-download-url file-id revision-id)
        dest (str (System/getProperty "java.io.tmpdir")
                  File/separator
                  "gdrive-clean-"
                  (UUID/randomUUID)
                  ".csv")]
    (log/info
     {:msg "starting Drive stream-download + clean"
      :metric
      {:file-id file-id
       :revision-id revision-id
       :url url
       :dest dest}})
    (let [request (build-request url token)
          response (.send http-client
                          request
                          (HttpResponse$BodyHandlers/ofInputStream))
          status (.statusCode response)]
      (when (or (< status 200)
                (>= status 300))
        (throw (ex-info "Drive download request failed"
                        {:status status
                         :url url})))
      (with-open [body-stream (.body response)]
        (stream-clean-to-file!
         body-stream
         dest
         options)))
    (.deleteOnExit (File. dest))
    (log/info
     {:msg "Drive stream-download + clean complete"
      :metric {:dest dest}})
    dest))