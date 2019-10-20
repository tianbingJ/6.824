
#小结

##并发编程
今天调试了很久的程序，有两个case都是因为并发编程引起的。
### 使用上下文明确的变量进行数据更新，不要使用++， --
具体case是心跳检测发送的AppendEntries和Replicate数据发送的AppendEntries()请求如果失败，会减少nextIndex：

```
rf.nextIndex[serv] 
```
这条语句在临界区内，初步考虑没有问题。但是如果两个请求的参数完全一致，相当于同样的请求由不同goroutine发送了两次，结果上述语句会把nextIndex[serv]的数值减2。
实际上rf.nextIndex[serv]的值应该减1.
改为如下可避免这个问题：
```$xslt
rf.nextIndex[serv] = nextLogIndex - 1
```
这样的意义是用上下文明确含义的信息去更新变量，重复执行这条语句不会产生副作用。而++和--这样的操作已经脱离了上下文，重复执行会得到累积的结果。

### Lock()和Unlock()的匹配问题
有些地方不适合使用defer mu.Unlock，比如在循环中，每次循环都需要Lock()和Unlock();在同一个循环中可能又有多个Unlock的地方：continue，return等前面需要手动添加unlock，很容就会哪里忘记添加Unlock使得死锁，或者已经释放锁了而再次尝试释放。
```$xslt
rf.mu.Lock()
if rf.state != Leader || rf.currentTerm != term {
   rf.mu.Unlock() //这里的锁经常忘记Unlock
   return
}
```
- 改进的方法1：循环里的内容启动一个goroutine去执行，并等待它执行结束。这样在goroutine中就可以使用defer语句了。
```$xslt
for () {
    chan c := make(chan int)
    go func(...){
        defer close(c)
        mu.Lock()
        defer.mu.Unlock()
        ...
    } (...)
    <- c
}

```
