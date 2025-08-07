example:
        cd /data/opentenbase/PG-XL-v10
	make -sj
        make uttest

notice:
        1> Install googletest-release-1.8.1.zip under root.
        2> configure with --enable-nls=C to avoid conflicts with gtest
        3> http://tapd.oa.com/pgxz/markdown_wikis/show/?#1210092131001971719
