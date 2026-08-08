# CHANGELOG

## 3.0.1 (2026-08-08)

* test: probe the server before trusting the fakes, and make the mutation score mean something by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/83f08f175efd0ca8a88217a3966fbddde27f3753)


## 3.0.0 (2026-08-08)

* fix: shutdown that never finished, and key events that half arrived by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/f8d703244cb7b6cb0a4bf9901f08626044611c01)
* test: cover the new shutdown and fan-out paths, and drop dead guards by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/56323c2f718ea2c1fc6312fb80cf1703ad251948)
* test: take coverage to 100% of lines and functions by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/8695a4ba651fd4dc10b9d1967b8771f430b8fdbf)


## 2.1.0 (2026-08-08)

* fix: cancel blocking reads on disconnect instead of hanging shutdown by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/09254065481ee3958e1558e80f6fa3021d01a71b)
* feat: xautoclaim, keyspace events and proof for the failover paths by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/6dd1a66d7ff3390a9b122d74d51170dbb27b92c0)
* feat: redis cluster support with MOVED/ASK redirection by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/35d2f98fbbb57a99c441575bcf52767ecc211812)
* test: prove recovery twice over, and while the server keeps flapping by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/5ce812949ffe1855f4da6b5a8300116660838547)


## 2.0.0 (2026-08-08)

* fix: stop dropping driver options, starting with tls by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/99e38b0aeea610b660246126c464a2b8288bac01)


## 1.2.0 (2026-08-08)

* test: drive time through a clock seam instead of sleeping by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/30890648a9bce952fa08344c5b08a53d2a6b8470)
* chore: sync development with the 1.1.0 release by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/53cc8fe4e399b78be50ca5f8ff252b14d644e883)
* chore: release automation and community files by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/8eaa006a59e208516ab84939838138340df1d106)


## 1.1.0 (2026-08-01)

* chore: align main with the v1.0.0 release [skip ci] by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/2e538851d416c58cbc2d1237ba60f8579057ae7c)
* test: kill test theater with mutation testing, fixing two real bugs by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/46e112847cc2755453c45d13587ee40ee10ddf9c)
* docs: add 16 runnable examples and fix keyPrefix on xgroup/xinfo by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/666447a77e0dd1907e48a6bd59738461a99d824a)
* feat: sorted sets with numeric scores and paired WITHSCORES results by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/bcd3e6f3ceaa7288b7ca17f7289569d7cae1829d)
* fix: address code-review findings on health cache and zadd by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/7085fc3529757bfafb2833cda671bdbbb728c2e2)
* fix: let awaited timers actually fire by Pedro Rogério [View](https://github.com/pinceladasdaweb/redis/commit/859b166acd7b6a44fec901b74ce88fdd04ec247c)
