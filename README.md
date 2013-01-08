# clj-tair

Clojure client for [Tair](https://github.com/taobao/tair)

## Usage

``` clojure
(def tair (mk-tair "b2bcomm-daily")  ;; create the tair client
(.init tair)                         ;; init the client
(def namespace 89)                   ;; we will use the namespace 89
(put tair namespace "key" "value")   ;; put something into tair
(get tair namespace "key")           ;; get something from tair
(delete tair namespace "key")        ;; delete something from tair
```

For more detail about the apis, please check the source code.

## License

Copyright (C) 2013 xumingming

Distributed under the Eclipse Public License, the same as Clojure.
