from ast import While
import concurrent.futures
import multiprocessing
import argparse
import logging.config
from time import sleep

from setting.LoggerConf import MY_LOGGING_CONFIG

import sniffer
import file_extractor

logging.config.dictConfig(MY_LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def test(index):
    print("daud here", index)
    sleep(5)
    print('done ', index)

def snifThread(input_file,input_interface,output_mode,output, label):
    try:
        session_instance = sniffer.createSniffer(
        input_file,
        input_interface,
        output_mode,
        output,
        label)

        session_instance.start()

        try:
            session_instance.join()
        except KeyboardInterrupt:
            session_instance.stop()
        finally:
            session_instance.join()

    except Exception as e:
        logger.info("Error while calling sniffer.")
        logger.exception(e)

    print("finish")

def main():
    parser = argparse.ArgumentParser(prog="mlridin",description='A Machine Learning based Real-time Intrusion Detection System in Network')

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-i','--interface',
    dest="input_interface",
    help="This interface will be used to capture traffic in order to convert it into the flow.")

    group.add_argument('-f','--file',
    dest="input_file",
    help="This file will be converted to the flow.")


    group.add_argument('--folder',
    dest="folder", help=" The path of folder containing pcaps files which need to convert to CSV Flows.")

    parser.add_argument('-c','--csv','--flow',action='store_const',const="flow", 
    dest="output_mode",
    help="The output will be store in the form of csv in output file.")

    parser.add_argument('--output-file',default='flow.csv',
    dest="output",
    help="default: %(default)s, The file output will be written to.")

    parser.add_argument('--class','--label',default='NeedManualLabel',
    dest="label",
    help="default: %(default)s, The entire generated CSV flow labeled with.")

    args = parser.parse_args()

    cores = multiprocessing.cpu_count()
    jobs = []
    if args.folder:
        fe = file_extractor.FileExtractor(args.folder)
        files = fe.getFiles()
        try:
            print('im visible?')
            while True:
                print('new while iteration')
                for i in range(cores):
                    file = next(files)
                    filename = f"csv/{file.name[:-5]}.csv"
                    print(filename)
                    p = multiprocessing.Process(target=snifThread, args=(file.path,args.input_interface,args.output_mode,filename, args.label,))
                    # p = multiprocessing.Process(target=test, args=(i,))
                    jobs.append(p)
                    p.start()
                # print('jobs here')
                # print(jobs)
                # print(len(jobs))
                for job in jobs:
                    # print('yesh')
                    job.join()
                    # print("each process merged")
                print("all process finished")
        except StopIteration:
            print("StopIteration")
            # for job in jobs:
            #     # print('yesh')
            #     job.join()
            #     print("everything finished")
        finally:
            pass
        print("done loop")
        # for (i, file) in enumerate(files):
        #     filename = f"csv/{file.name[:-5]}.csv"
        #     snifThread(file.path,args.input_interface,args.output_mode,filename, args.label)

    else:
        snifThread(args.input_file,args.input_interface,args.output_mode,args.output, args.label)

if __name__ == "__main__":
    main()