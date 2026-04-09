(ns connector.cloud-fetch
  "Provides HTTP file download utilities for GDrive, Dropbox, and public URLs."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.timbre :as log])
  (:import
   [java.io File]
   [java.net URI]
   [java.net.http
    HttpClient HttpClient$Redirect HttpRequest HttpResponse$BodyHandlers]
   [java.time Duration]
   [java.util UUID]))

(def http-client
  "Reusable HTTP client configured with timeout and redirect handling."
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofSeconds 30))
      (.followRedirects HttpClient$Redirect/NORMAL)
      (.build)))

(defn- gdrive-id
  "Extracts Google Drive file ID from supported URL patterns."
  [url]
  (or (some (fn [pat]
              (when-let [id (re-find pat url)]
                (second id)))
            [#"/file/d/([a-zA-Z0-9_-]+)"
             #"[?&]id=([a-zA-Z0-9_-]+)"
             #"/open\\?id=([a-zA-Z0-9_-]+)"])
      (throw
       (ex-info "Unable to extract Google Drive file ID" {:url url}))))

(defn- refresh-access-token
  "Fetches new Google OAuth access token using refresh credentials."
  [refresh-token client-id client-secret]
  (let [body (str "client_id=" client-id
                  "&client_secret=" client-secret
                  "&refresh_token=" refresh-token
                  "&grant_type=refresh_token")
        req (-> (HttpRequest/newBuilder)
                (.uri (URI/create "https://oauth2.googleapis.com/token"))
                (.timeout (Duration/ofSeconds 30))
                (.header "Content-Type" "application/x-www-form-urlencoded")
                (.POST (java.net.http.HttpRequest$BodyPublishers/ofString body))
                (.build))
        res (.send http-client req (HttpResponse$BodyHandlers/ofString))]
    (if (= 200 (.statusCode res))
      (second (re-find #"\"access_token\"\s*:\s*\"([^\"]+)\"" (.body res)))
      (throw (ex-info "Unable to refresh access token"
                      {:status (.statusCode res)})))))

(defn gdrive-req
  "Builds Google Drive request supporting public, private, and revision
   download."
  [link refresh-token client-id client-secret revision-id]
  (let [id (gdrive-id link)]
    (cond
      revision-id
      (let [token (refresh-access-token refresh-token client-id client-secret)]
        {:url (str "https://www.googleapis.com/drive/v3/files/" id
                   "/revisions/" revision-id "?alt=media")
         :method  :get
         :headers {"Authorization" (str "Bearer " token)}})

      ;; Private file
      (and refresh-token client-id client-secret)
      (let [token (refresh-access-token refresh-token client-id client-secret)]
        {:url (str "https://www.googleapis.com/drive/v3/files/" id "?alt=media")
         :method :get
         :headers {"Authorization" (str "Bearer " token)}})

      ;; Public file
      :else
      {:url (str "https://drive.google.com/uc?export=download&id=" id)
       :method :get
       :headers {}})))

(defn dropbox-req
  "Builds Dropbox request supporting public link, private token, and revision
   download."
  [link token revision-id]
  (cond
    ;; Revision download
    revision-id
    {:url  "https://content.dropboxapi.com/2/files/download"
     :method :post
     :headers {"Authorization" (str "Bearer " token)
               "Dropbox-API-Arg" (str "{\"path\": \"rev:" revision-id "\"}")}}

    ;; Private file
    token
    {:url "https://content.dropboxapi.com/2/files/download"
     :method :post
     :headers {"Authorization" (str "Bearer " token)
               "Dropbox-API-Arg" (str "{\"path\": \"" link "\"}")}}

    ;; Public link
    :else
    {:url (cond
            (str/includes? link "dl=0") (str/replace link "dl=0" "dl=1")
            (str/includes? link "?") (str link "&dl=1")
            :else (str link "?dl=1"))
     :method :get
     :headers {}}))

(defn fetch-file!
  "Downloads remote file to a temporary local path and returns the file
   location."
  [src cred]
  (let [{:keys [link token revision-id refresh-token client-id client-secret]}
        cred
        {:keys [url method headers]}
        (case src
          :gdrive
          (gdrive-req link refresh-token client-id client-secret revision-id)

          :dropbox
          (dropbox-req link token revision-id)

          {:url link :method :get :headers {}})

        dest (str (System/getProperty "java.io.tmpdir") File/separator
                  "cloud-raw-"
                  (UUID/randomUUID)
                  ".csv")]
    (.deleteOnExit (File. dest))

    (log/info {:msg "Starting cloud download"
               :metric {:type src
                        :url  url
                        :dest dest}})

    (let [start (System/currentTimeMillis)
          req-builder (HttpRequest/newBuilder)]

      (.uri req-builder (URI/create url))
      (.timeout req-builder (Duration/ofSeconds 120))

      (doseq [[k v] headers]
        (.header req-builder k v))

      (let [request (if (= method :post)
                      (.build
                       (.POST
                        req-builder
                        (java.net.http.HttpRequest$BodyPublishers/noBody)))
                      (.build (.GET req-builder)))
            response (.send http-client
                            request
                            (HttpResponse$BodyHandlers/ofInputStream))
            status (.statusCode response)]

        (when (or (< status 200) (>= status 300))
          (throw
           (ex-info "Cloud download failed"
                    {:status status
                     :url url
                     :type src})))

        (with-open [in (.body response)
                    out (io/output-stream dest)]
          (io/copy in out))

        (log/info {:msg "Download complete"
                   :metric {:type src
                            :bytes (.length (File. dest))
                            :duration-ms
                            (- (System/currentTimeMillis) start)}})))
    dest))