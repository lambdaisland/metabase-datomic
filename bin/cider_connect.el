(let ((repl-buffer
       (cider-connect-clj (list :host "localhost"
                                :port "4444"
                                :project-dir "/home/arne/github/metabase"))))

  (sesman-link-session 'CIDER
                       (list "github/metabase:localhost:4444" repl-buffer)
                       'project
                       "/home/arne/github/metabase-datomic/"))

;;(sesman-browser)
