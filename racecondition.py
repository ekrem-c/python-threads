import logging
import threading
import time
import concurrent.futures

l = threading.Lock()

class FakeDatabase:
    def __init__(self):
        self.value = 0

    def update(self, name): #simulating doing some work on database and updating the value
        logging.info("Thread %s: starting update", name)
        l.acquire() #using mutex locks strategically to prevent race conditions
        local_copy = self.value
        local_copy += 1
        time.sleep(0.1)
        self.value = local_copy
        l.release()
        logging.info("Thread %s: finishing update", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    database = FakeDatabase()
    logging.info("Testing update. Starting value is %d.", database.value)
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        for index in range(200):
            executor.submit(database.update, index) #submit function allow us to pass arguments to thread functions
    logging.info("Testing update. Ending value is %d.", database.value)



#Good to know: threading.Semaphore(3) allows you to have x amount of mutex locks if you have limited sources and only a fixed amount of threads to use those resources
#Another tip: Using threading.Timer(x), my_function) will start your timer after x seconds later.