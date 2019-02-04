#---------------------------------------------------
# Created: Feb 3, 2019
# Tested on: NiFi 1.8.0, Python 2.7.10 and 3.6.5
#---------------------------------------------------

import json
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback, InputStreamCallback, OutputStreamCallback

#---------------------------------------------------
# START
#---------------------------------------------------

# class to streamline interactions with NiFi flow files such as:
#     updating flow file content
#     generating child flow files
#     updating, adding, removing flow file attributes

#---------------------------------------------------
class nifi():

    #---------------------------------------------------
    def __init__(self, flowFile = None):

        # parent class initiated from original flow file
        # child objects initiated from input flow file (with attributes and content)
        # "global content" required to pass content through callback subclass 

        global content
        if flowFile == None:
            self.flowFile = session.get()
            self.putAttribute('parent', 'True')
            content = self.nifiContent()
            self.flowFile = session.write(self.flowFile, self.getStreamContent())
            self.content = content
            self.children = []
        else:
            self.flowFile = flowFile
            self.content = None
        self.attributes = self.flowFile.getAttributes() #dictionary of attributes
    
    #---------------------------------------------------
    def child(self, content = None):

        # children inherit attributes of parent flow file
        # content for child flow file initiates as None 
        # child objects cannot create child objects

        if self.flowFile.getAttribute('parent') == 'False':
            return

        self.children.append(nifi(session.create(self.flowFile)))
        self.children[-1].putAttribute('childNum', len(self.children))
        self.children[-1].putAttribute('parent', 'False')
        if content != None:
            self.children[-1].write(content)

    #---------------------------------------------------
    def transfer(self, relationship):

        # nifi processor "ExecuteScript" only allows two relationships:
	#     REL_SUCCESS and REL_FAILURE

        if relationship.lower() == 'success':
            session.transfer(self.flowFile, REL_SUCCESS)
        elif relationship.lower() == 'failure':
            session.transfer(self.flowFile, REL_FAILURE)
        else:
            raise Exception('Unapproved Relationship')

    #---------------------------------------------------
    def commit(self):

        # all changes to flowfiles must be commited to close

        for k in range(len(self.children)):
            self.children[k].putAttribute('numChildren', len(self.children))
            self.children[k].transfer('success')
        self.putAttribute('numChildren', len(self.children))
        self.transfer('success')
        session.commit()

    #---------------------------------------------------
    def getAttribute(self, attr):

        # get value for attribute "attr"
        # if attribute does not exist, return "None"

        if attr in self.attributes.keys():
            return self.flowFile.getAttribute(attr)
        else:
            return None

    #---------------------------------------------------
    def putAttribute(self, attr, value):

        # create attribute "attr" with string version of "value"
        # if "attr" already exists, the existing value is replaced
        # cannot update value for uuid attribute

        if attr != 'uuid':
            self.flowFile = session.putAttribute(self.flowFile, attr, str(value))

    #---------------------------------------------------
    def removeAttribute(self, attr):

        # remove attribute from flow file
        # cannot remove uuid attribute        

        if ((attr != 'uuid') and (attr in self.attributes)):
            self.flowFile = session.removeAttribute(self.flowFile, attr)

    #---------------------------------------------------
    def removeAllAttributes(self):

        #remove all attributes except uuid

        for attr in self.attributes.keys():
            self.removeAttribute(attr)

    #---------------------------------------------------
    def write(self, output):

        # write "output" to contents of flow file
        # "output" need not be a string

        self.content = output
        self.flowFile = session.putAttribute(self.flowFile, "content", self.content)
        self.flowFile = session.write(self.flowFile, self.outputWrite(self.flowFile))
        self.flowFile = session.removeAttribute(self.flowFile, "content")

    #---------------------------------------------------
    class nifiContent():

        # helper subclass to convert Java IO stream from flow file contents

        def __init__(self):
            self.stream = None
        def toString(self):
            return ''.join(map(chr, self.stream))
        def toJson(self):
            return json.loads(self.toString())

    #---------------------------------------------------
    class outputWrite(StreamCallback, OutputStreamCallback):
        
        # subclass to write content to flow file contents

        def __init__(self, flowFile):
            self.flowFile = flowFile
        def process(self, outputStream):
            outputStream.write(bytearray(self.flowFile.getAttribute("content").encode("UTF-8")))

    #---------------------------------------------------
    class getStreamContent(StreamCallback):

        # subclass to get content from flow file

        def __init__(self):
            pass
        def process(self, inputStream, outputStream):
            global content
            content.stream = IOUtils.toByteArray(inputStream)

            #do stuff to original file:
            outputContent = content.stream
            #-------

            outputStream.write(outputContent) #keep original content in parent file

#---------------------------------------------------
# END
#---------------------------------------------------

#---------------------------------------------------
# Script to Execute
#---------------------------------------------------

# initiate class
n = nifi()

# convert contents to string
content = n.content.toString()

# create child flow file with no content
n.child()

# add new attribute to parent flow file
n.putAttribute('beforeChild', 'HHHH')

# create multiple child flow files with content
# child flow files inherit all parent attributes at time of creation
for p in content.split('A'):
    n.child(p)

# add new attribute to parent flow file
n.putAttribute('afterChild', '123123')

# push all changes to output stream
n.commit()

#---------------------------------------------------
# End of Script
#---------------------------------------------------
