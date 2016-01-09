"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
"""
   NAME:
      addcopyright
   PURPOSE:
      Prepend a licence, copyright and contract acknowledgement to every
      file in a codebase.
   REVISION HISTORY:
      08 Jan 2016  Alexia
"""
import os

def listfiles(pth):
  """Create the file list for prepending the appropriate license"""
  fl=[]
  for path, dirs, files in os.walk(pth):
    print path
    for f in files:
      fl.append(path+'/'+f)

  return fl

def acr(fl):
  """for each file in a file list, prepend the appropriate license"""

  pycommand  =lambda fn: 'cat NOTICE '+fn+' > tmp && mv tmp '+ fn 
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

def main(pth):
  """Execute adding the license statement to all files"""
  fl=listfiles(pth)
  oddfiles=acr(fl)

  with open('oddfiles.txt','w') as f:
    for fi in oddfiles:
      f.write(fi+'\n')

  return
