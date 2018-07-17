# coding:utf-8

"""
@author: kevin

@contact: kevin_678@126.com

@file: config.py

@time: 2017/6/15 18:11

@desc:

@Software: data-analysis

"""
import subprocess
import sys


class ExecuteShell(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''

    def executeShell(self, shell_cmd):
        return_code = -2
        try:
            process = subprocess.Popen(shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            (stdout, stderr) = process.communicate()
            return_code = process.poll()
        except Exception as e:
            print "--- executeSell() --- : {}".format(e)
            raise e

        print "[ SHELL ] ------------------------- ERROR-START -------------------------"
        print stderr
        print "[ SHELL ] ------------------------- ERROR-END -------------------------"
        print "[ SHELL ] ------------------------- INFOMATION-START -------------------------"
        print stdout
        print "[ SHELL ] ------------------------- INFOMATION-END -------------------------"

        return (return_code, stdout, stderr)


if __name__ == '__main__':
    ex = ExecuteShell()

    cmd = 'ping www.baidu.com'
    #     lis=ex.getStdOutList_out(cmd)
    (returncode, stdoutput, erroutput) = ex.executeShell(cmd)

    print "++++++++++++++++++++++++++++++++++++++++++++++++++"
    print stdoutput.decode("gbk")
    print erroutput.decode("gbk")

    print sys.getfilesystemencoding()
    print sys.getdefaultencoding()
