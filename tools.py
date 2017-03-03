# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 09:55:33 2017
@author: ana
"""

import os
import re
import win32api
import threading
import shutil
import arrow

class Dolphin():
    def __init__(self, link, create=False):
        if os.path.exists(link) == False:
            if create == True:
                os.mkdir(link)
            else:
                raise ValueError
        
        self.path = link
        
    @property
    def parent(self):
        return os.path.dirname(self.path)
    
    def exist(self,filename):
        if filename not in self.files():
            return False
        else:
            return True
    
    def files(self, *args, reverse=False, part=None):
        # out:file names
        # part:=all or [1:n]
        # reverse:= [1,2,3] -- [3,2,1]
        if args is None:
            args = []
        try:
            full = len(os.listdir(self.path))
            # list of file names
            files = os.listdir(self.path)

            # subset < full set
            if part is not None and part > full:
                raise KeyboardInterrupt

            # filter the extentions
            if len(args) != 0:
                temp = []
                for f in files:
                    if os.path.splitext(f)[1] in args:
                        temp.append(f)
                files = temp

            if reverse is True:
                files.reverse()

            if part is None:
                self._files = files
            else:
                self._files = files[:part]
        except ValueError:
            return ValueError
        except KeyboardInterrupt:
            return ValueError
        return self._files

        # f=FileCommander(r'C:\Users\crava\Desktop\pyexcel_sql-alpha1')
        #print(f.files())
        
    def joinpath(self,path):
        if os.path.isfile(self.path + '\\' + path):
            return self.path + '\\' + path
        else:
            return Dolphin(self.path + '\\' + path)
            
        
    def excute(self,filename):
        if self.exist(filename) == True:
            win32api.ShellExecute(0, 'open', self.joinpath(filename), '', '', 1)
        else:
            raise ValueError
    
    # filename --> folder + filename
    # just give the filenames of the current folder
    def copy(self,*filenames,folder):
        for fi in filenames:
            if self.exist(fi) == False:
                raise ValueError
        if os.path.exists(folder) == False:
            os.mkdir(folder)
        if len(filenames) == 1:
            shutil.copy(src=self.joinpath(filenames[0]),dst=folder)
        else:
            try:
                threads=[]
                for fi in filenames:
                    t = threading.Thread(target=shutil.copy,args=(self.joinpath(fi),folder))
                    threads.append(t)
                for th in threads:
                    th.start()
                th.join()
            except Exception as e:
                print(e)
    
    def distribute(self,*kargs):
        raise NotImplementedError
        
    def rename(self,filename,name):
        if filename is None:
            os.rename(self.path,self.parent+'//'+name)
        else:
            os.rename(self.joinpath(filename),self.path+name)
            
class Transladdr:
    def __init__(self, rng):
        """ Read a string range "A1:B2"--> [(A,1),(B,2)] """
        # like "A1:B1"
        sep = rng.find(':')
        if sep != -1:
            begin = rng[:sep]  # A1
            end = rng[sep + 1:]  # B2
            
            column1, row1 = self.split(begin)
            column2, row2 = self.split(end)
            # pack the values
            self.ranges = [(column1, row1), (column2, row2)]  
        # like "A1"
        else:
            column, row = self.split(rng)
            self.ranges = [(column, row)]
    
    @property  
    def cells(self):
        """[(B,1)]-->[(1,2)]""" 
        if len(self.ranges) == 2:
            assert type(self.ranges[0][0]) == str
            column1 = self.col_numeric(self.ranges[0][0])
            assert type(self.ranges[1][0]) == str
            column2 = self.col_numeric(self.ranges[1][0])
            return [(self.ranges[0][1], column1), (self.ranges[1][1], column2)]
        else:
            assert type(self.ranges[0][0]) == str
            column = self.col_numeric(self.ranges[0][0])
            return [(self.ranges[0][1], column)]
    
    @property
    def col(self):
        columns = [item[0] for item in self.ranges]  # [A] or [A,B,C...]
        return {i: self.col_numeric(i) for i in columns}  # {A:1,B:2,C:3 ...}
    
    @property
    def column_list(self):
        if len(self.cells) == 2:  # [(A,2),(C,4)] -- [(2,1),(4,3)]
            ls = []
            begin = self.cells[0][1]
            end = self.cells[1][1]
            length = end - begin
            if length > 0:
                for i in range(0, length):
                    ls.append(self.col_char(i + begin))
            ls.append(chr(end + 64))
            return ls
        else:
            raise ValueError
        
    def length(self, axis=0):
        if len(self.cells) == 2:
            if axis == 0:  # in rows
                return abs(self.cells[0][0] - self.cells[1][0])
            elif axis == 1:
                return abs(self.cells[0][1] - self.cells[1][1])
        else:
            return 0
        
    def next(self,direction='right'):
        raise NotImplementedError
        
    @staticmethod
    def split(address):

        # if the first is not $ but have $ in other position -> Error
        if address.find('$') > 0:
            raise ValueError

        # if $A$1
        if address.find('$') == 0:
            try:
                column = re.findall('\$(\D+)\$', address)[0].upper()  # A
                row = re.findall('\d+', address)[0]  # 1
                return column, int(row)  # A,1
            except:
                raise AttributeError

        # if A1
        if address.find('$') == -1:
            column = re.findall('\D+', address)[0].upper()
            row = re.findall('\d+', address)[0]
            return column, int(row)
            
    @staticmethod
    def col_numeric(char) -> int:
        # [AB]-- 28
        result = 0
        ls = [c for c in char]  # ls:=[A,B]
        length = len(ls)
        for i in range(0, length):
            weight = 26 ** (length - 1 - i)  # [26**i,26**i-1,...26**1,26**0]
            number = ord(ls[i]) - 64  # [1...26]
            result += weight * number
        return result

    @staticmethod
    def col_char(num):
        if num < 1:
            raise ValueError("Index is too small")
        result = ""
        while True:
            if num > 26:
                num, r = divmod(num - 1, 26)
                result = chr(r + ord('A')) + result
            else:
                return chr(num + ord('A') - 1) + result
            
    @staticmethod
    def entire(obj):
        if type(obj) == str:
            return '{o}:{o}'.format(o=obj)
        else:
            try:
                obj = str(obj)
                return '{o}:{o}'.format(o=obj)
            except Exception as e:
                print(e)
    
    
class Calender():
    def __init__(self,line):
        if len(re.findall('\D',line)) == 0:
            print('Cannot input YYYYMMDD')
            raise ValueError
        if line is None:
            self.time = arrow.utcnow()
        else:
            self.time = arrow.get(line)

    @property
    def last_year(self):
        return self.time.shift(years=-1)
    
    @property
    def last_month(self):
        return self.time.shift(months=-1)
    
    @property
    def last_week(self):
        return self.time.shift(weeks=-1)
    
    @property
    def next_month(self):
        return self.time.shift(months=1)
    
    @property
    def next_next_month(self):
        return self.time.shift(months=2)
    
    @staticmethod
    def last_weekday(time,name='th'):
        name = name.upper()
        backward = {'MO':1,'TU':2,'WE':3,'TH':4,'FR':5,'SA':6,'SU':7}
        while time.isoweekday() != backward[name] :
            time = time.shift(days = backward)
        return time