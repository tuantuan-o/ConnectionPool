#include "pch.h"
#include "CommonConnectionPool.h"
#include "public.h"

// 线程安全的懒汉单例函数接口
ConnectionPool* ConnectionPool::getConnectionPool()
{
	static ConnectionPool pool; // lock和unlock
	return &pool;
}

// 从配置文件中加载配置项
bool ConnectionPool::loadConfigFile()
{
	FILE *pf = fopen("mysql.ini", "r");
	if (pf == nullptr)
	{
		LOG("mysql.ini file is not exist!");
		return false;
	}

	while (!feof(pf))
	{
		char line[1024] = { 0 };
		fgets(line, 1024, pf);
		string str = line;
		int idx = str.find('=', 0);
		if (idx == -1) // 无效的配置项
		{
			continue;
		}

		// password=123456\n
		int endidx = str.find('\n', idx);
		string key = str.substr(0, idx);
		string value = str.substr(idx + 1, endidx - idx - 1);

		if (key == "ip")
		{
			_ip = value;
		}
		else if (key == "port")
		{
			_port = atoi(value.c_str());
		}
		else if (key == "username")
		{
			_username = value;
		}
		else if (key == "password")
		{
			_password = value;
		}
		else if (key == "dbname")
		{
			_dbname = value;
		}
		else if (key == "initSize")
		{
			_initSize = atoi(value.c_str());
		}
		else if (key == "maxSize")
		{
			_maxSize = atoi(value.c_str());
		}
		else if (key == "maxIdleTime")
		{
			_maxIdleTime = atoi(value.c_str());
		}
		else if (key == "connectionTimeOut")
		{
			_connectionTimeout = atoi(value.c_str());
		}
	}
	return true;
}

// 连接池的构造
ConnectionPool::ConnectionPool()
{
	// 加载配置项了
	if (!loadConfigFile())
	{
		return;
	}

	// 创建初始数量的连接
	for (int i = 0; i < _initSize; ++i)
	{
		Connection *p = new Connection();
		p->connect(_ip, _port, _username, _password, _dbname);
		p->refreshAliveTime(); // 刷新一下开始空闲的起始时间
		_connectionQue.push(p);
		_connectionCnt++;
	}

	// 启动一个新的线程，作为连接的生产者 linux thread => pthread_create
	thread produce(std::bind(&ConnectionPool::produceConnectionTask, this));
	produce.detach();

	// 启动一个新的定时线程，扫描超过maxIdleTime时间的空闲连接，进行对于的连接回收
	thread scanner(std::bind(&ConnectionPool::scannerConnectionTask, this));
	scanner.detach();
}

// 运行在独立的线程中，专门负责生产新连接
void ConnectionPool::produceConnectionTask()
{
	for (;;)
	{
		unique_lock<mutex> lock(_queueMutex);
		while (!_connectionQue.empty())
		{
			cv.wait(lock); // 队列不空，此处生产线程进入等待状态
		}

		// 连接数量没有到达上限，继续创建新的连接
		if (_connectionCnt < _maxSize)
		{
			Connection *p = new Connection();
			p->connect(_ip, _port, _username, _password, _dbname);
			p->refreshAliveTime(); // 刷新一下开始空闲的起始时间
			_connectionQue.push(p);
			_connectionCnt++;
		}

		// 通知消费者线程，可以消费连接了
		cv.notify_all();
	}
}


// 给外部提供接口，从连接池中获取一个可用的空闲连接
shared_ptr<Connection> ConnectionPool::getConnection()
{
	unique_lock<mutex> lock(_queueMutex);
	while (_connectionQue.empty())
	{
		// 不是sleep
		if (cv_status::timeout == cv.wait_for(lock, chrono::milliseconds(_connectionTimeout)))
		{
			if (_connectionQue.empty())
			{
				LOG("获取空闲连接超时了...获取连接失败!");
					return nullptr;
			}
		}
	}

	/*
	shared_ptr智能指针析构时，会把connection资源直接delete掉，相当于
	调用connection的析构函数，connection就被close掉了。
	这里需要自定义shared_ptr的释放资源的方式，把connection直接归还到queue当中
	*/
	shared_ptr<Connection> sp(_connectionQue.front(), 
		[&](Connection *pcon) {
		// 这里是在服务器应用线程中调用的，所以一定要考虑队列的线程安全操作
		unique_lock<mutex> lock(_queueMutex);
		pcon->refreshAliveTime(); // 刷新一下开始空闲的起始时间
		_connectionQue.push(pcon);
	});

	_connectionQue.pop();
	cv.notify_all();  // 消费完连接以后，通知生产者线程检查一下，如果队列为空了，赶紧生产连接
	
	return sp;
}

// 扫描超过maxIdleTime时间的空闲连接，进行对于的连接回收
void ConnectionPool::scannerConnectionTask()
{
	for (;;)
	{
		// 通过sleep模拟定时效果
		this_thread::sleep_for(chrono::seconds(_maxIdleTime));

		// 扫描整个队列，释放多余的连接
		unique_lock<mutex> lock(_queueMutex);
		while (_connectionCnt > _initSize)
		{
			Connection *p = _connectionQue.front();
			if (p->getAliveeTime() >= (_maxIdleTime * 1000))
			{
				_connectionQue.pop();
				_connectionCnt--;
				delete p; // 调用~Connection()释放连接
			}
			else
			{
				break; // 队头的连接没有超过_maxIdleTime，其它连接肯定没有
			}
		}
	}
}

/*
	
	生产者线程实现了什么东西？
	生产者线程专门负责产生新连接，涉及到队列操作，需要保证线程安全，首先声明一把互斥锁，然后检查队列是否为空，如果不为空就进入等待状态，
	等待消费者消费，cv.wait(lock)，这时生产者进程会将锁释放，让消费者线程拿到此锁，取走连接，当消费者线程消费完以后，会notify生产者线程，
	它检查队列是否为空，若为空了就开始进行下一步检查，检查可用连接是否达到上限maxsize，若达到上限就不创建，
	若没有达到上限就进行创建，创建后再notify消费者进行消费。

	cv.wait(lock)，进入等待状态的时候，会释放这把锁，消费者线程就从等待状态变为阻塞状态，哪条线程先拿到这把锁，哪条线程就从队列中取出连接。

	消费者线程怎么实现的？
	消费者线程消费连接，涉及到队列操作，需要保证线程安全，所以首先会声明一把互斥锁，然后检查队列是否为空，若队列为空，就等待配置文件指定的
	超时时间，若在这个时间内被唤醒了(被notify了)，就检查队列是否为空，若不为空就消费连接，如果经过超时时间后队列还是空的，就输出超时，获取
	连接失败。接下来就是消费连接的过程，消费的过程中需要将连接取走，即从队列中pop掉此连接，随后return 这个连接，在其他函数中进行sql语句调用
	然后通知生产者检查队列是否为空，若队列为空则进行生产。这里有关于connection类的析构函数，即取出连接的时候可以使用智能指针维护连接类，
	在shared_ptr<Connection>(1,2)，在2的位置为自定义删除器功能，将原本智能指针实现的释放资源改写为将连接资源归还到队列的末尾，
	由于此过程设计服务器应用线程，需要考虑线程安全，因此在lambda表达式中也需要添加互斥锁。

	while(_connectionQue.empty()){
		if (cv_status::timeout == cv.wait_for(lock, chrono::milliseconds(_connectionTimeout)))
		...
	}
	上述的代码实现的功能：当队列为空时，就等待最大等待时间，于此同时通知生产者生产连接，如果生产者notify它了，即代表队列不为空了，
	那么就会跳出while语句，去消费连接。如果经过最大等待时间后，cv.wait_for返回timeout状态，即代表获取连接超时了，获取连接失败。


	连接回收：扫描超过maxIdleTime时间的空闲连接进行释放
	首先进入一个循环，通过sleep模拟睡眠效果，随后扫描整个队列，释放多余的连接，由于涉及到队列的删除，需要使用互斥锁。当前使用的连接量
	大于初始连接量时，就需要进行连接回收，首先取出队头的连接，当队头的连接在maxIdleTime内都没有被用到，就将队头的连接delete，
	这里的delete就调用connection的析构函数，即释放连接，同时将当前使用的连接计数cnt--

*/