# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS i
# IN THE SOFTWARE.

"""
   NAME:
      addcopyright
   PURPOSE:
      Prepend a licence, copyright and contract acknowledgement to every
      file in a codebase.
   REVISION HISTORY:
      08 Jan 2016  Alexia
      01 Jun 2018  -- Added a cleanup function to remove the copyright info
      01 Jun 2018  -- Adjusted cleanup function, it mangled a few files before,
                      some will have to be manually dealt with. 
"""
import os

def listfiles(pth):
  """Create the file list for prepending the appropriate license"""
  fl=[]
  for path, dirs, files in os.walk(pth):
    #print path
    for f in files:
      fl.append(path+'/'+f)

  return fl

def acr(fl):
  """for each file in a file list, prepend the appropriate license"""

  # I don't remember why we were doing it this way
  # I am chaning it back to the ## method so these 
  # copyrights don't wind up in the doc-strings --A
  # pycommand  =lambda fn: 'cat NOTICE '+fn+' > tmp && mv tmp '+ fn 
  
  pycommand =lambda fn: 'cat NOTICE_yml '+fn+' > tmp && mv tmp '+ fn 
  ymlcommand =lambda fn: 'cat NOTICE_yml '+fn+' > tmp && mv tmp '+ fn 

  otherfiletype=[]

  for fi in fl:
    if fi=='./addcopyright.py':
      continue
    if fi[-3:]=='.py':
      os.system(pycommand(fi))
      continue
    if fi[-4:]=='.yml':
      os.system(ymlcommand(fi))
      continue
    if fi[-4:]=='.txt':
      os.system(ymlcommand(fi))
      continue
    if fi[-4:]=='.pyc':
      continue
    if fi[-5:]=='.json':
      continue
    otherfiletype.append(fi)

  return otherfiletype

def clean_apache(pth,killit=0):
  """Remove the Apache notices if they exist"""
  fl = listfiles(pth)
  apachestring = "Licensed under the Apache License, Version 2.0"

  def findlicense(fi):
    """Open file and search for Apache string"""
    with open(fi,'r') as f:
      lines=f.readlines()
      #print 'filename is ', fi
      #print lines[5:9]
      for line in lines[5:9]:
        if (apachestring in line):
          return True

      return False

  def compareit(fi):
    """Line by line compare to Apache License"""
    apachefile="/Users/al24406/filesync/Documents/MARC/SCAPE/SCAPE_test_7June2018/SCAPE/copyright/apachenotices/NOTICE_COPIED"
   
    flag=True
    with open(fi,'r') as f1:
      with open(apachefile,'r') as f2:
        for i in range(16):
          l1=f1.readline()
          l2=f2.readline()
          if l1==l2:
            continue
          else:
            flag=False
            break
    return flag

  # Check and then delete lines
  killcommand = lambda fn: 'tail -n 16 < '+fn+' > tmp && mv tmp '+ fn
  for f in fl:
    if findlicense(f):
      if compareit(f):
        print 'Working on file: ', f
        if killit: 
          os.system(killcommand(f))
      else:
        print "This file is weird!" , f
        with open('weirdfiles.txt','wa') as f3:
          f3.write(f + '\n')

  return

def main(pth):
  """Execute adding the license statement to all files"""
  fl=listfiles(pth)
  oddfiles=acr(fl)

  with open('oddfiles.txt','w') as f:
    for fi in oddfiles:
      f.write(fi+'\n')

  return
