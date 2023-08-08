from utils.worker import WorkerPool


def main():
    workers = WorkerPool()
    workers.run()

if __name__ == '__main__':
    main()

