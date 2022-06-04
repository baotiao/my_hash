

测试 32 thread
绑定32 thread 在同一个socket 下
mpmc:

这种绑定方法是在同一个socket 下 16个物理核32个逻辑核上, 也就是有2个thread 会共用1个物理核
taskset -ac 0-15,32-47 ./a.out
insert 16000000 elements, time cost 6536316 us

对比如果放在两个socket 下, 32个物理核的32个逻辑核上

taskset -ac 0-31 ./a.out
insert 16000000 elements, time cost 8078345 us


对比于stl_queue

stl_queue

taskset -ac 0-31 ./a.out
insert 16000000 elements, time cost 5457384 us
taskset -ac 0-15,32-47 ./a.out
insert 16000000 elements, time cost 7163720 us

std_queue 和mpmc 不一样, 跨socket 的性能反而是更好的


测试 16 thread 的话 mpmc 会比stl_queue 好更多, 原因是32 thread 的时候两种绑定方法要么跨socket, 要么要共用物理核, 16thread 可以绑定在一个socket 下的16个物理核上

mpmc:

taskset -ac 0-15 ./a.out
insert 8000000 elements, time cost 1760784 us

stl_queue:
taskset -ac 0-15 ./a.out
insert 8000000 elements, time cost 3056000 us

可以看到这里mpmc 是远远优于 stl_queue 的
