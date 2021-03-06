import logging
import threading
import time
import concurrent.futures

def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    # logging.info("Main    : before creating thread")
    # x = threading.Thread(target=thread_function, args=(1,), daemon = True) #try with Daemon = True as well, program will exit even in the presence of running threads
    # logging.info("Main    : before running thread")
    # x.start()
    # logging.info("Main    : wait for the thread to finish")
    # #x.join()   #if you want to wait a response from thread before proceeding
    # logging.info("Main    : all done")

    with concurrent.futures.ThreadPoolExecutor(max_workers=9) as executor:
        executor.map(thread_function, range(9))