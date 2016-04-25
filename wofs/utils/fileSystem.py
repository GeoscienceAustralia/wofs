import os
import tempfile

class FileSystemObject(object) :
    """ A directory or a file identified by its path
    """

    def __init__(self, path):
        self._path = os.path.abspath(path)

    def getPath(self) :
        """Return the full path of this file system object"""
        return (self._path)

    def getName(self) :
        """Return the base name of this file system object"""
        return os.path.basename(self.getPath())

    def exists(self) :
        """Test if this file system object exists"""
        return os.path.exists(self.getPath())

    def getParent(self) :
        """Return the parent directory of this object
           or None if this is the root directory"""
        if self.getPath() == "/" : 
            return None
        return Directory(os.path.dirname(self.getPath()))

    def chmod(self, mode) :
        os.chmod(self.getPath(), mode)

    def __str__(self) :
        return "FileSystemObject: %s" % self.getPath()
 
    def __eq__(self, other) :
        return self.getPath() == other.getPath()
 
    def __ne__(self, other) :
        return self.getPath() != other.getPath()
        



class Directory(FileSystemObject):
    """Represents a directory in the file system"""

    @staticmethod
    def getTempDirectory():
        return Directory(tempfile.mkdtemp())

    def __init__(self, path):
        super(Directory, self).__init__(path)

    def __str__(self) :
        return "Directory: %s" % self.getPath()

    def makedirs(self) :
        if not self.exists():
            os.makedirs(self.getPath())

    def delete(self):
        os.rmdir(self.getPath())

    def listFiles(self) :
        """Return a list of FileSystemObjects representing the contents of this directory"""
        result = []
        for f in os.listdir(self.getPath()) :
            path = os.path.join(self.getPath(), f)
            if os.path.isdir(path) :
                result.append(Directory(path))
            else:
                result.append(File(path))
        return result

    def accept(self, aFileObjectVisitor) :
        """Visitor Pattern support. Accept the supplied visitor
        and call it's visitDirectory method with this Directory as parameter.
        then get each member of this directory to accept the visitor"""

        aFileObjectVisitor.visitDirectory(self)

        # iterate over the file system objects in this directory
        # asking each to accept this visitor

        for fso in self.listFiles() :
            fso.accept(aFileObjectVisitor)
     
    
class File(FileSystemObject):
    """Represents a file in the file system"""

    def __init__(self, path):
        super(File, self).__init__(path)

    def __str__(self) :
        return "File: %s" % self.getPath()

    def delete(self):
        os.remove(self.getPath())

    def accept(self, aFileObjectVisitor) :
        """Visitor Pattern support. Accept the supplied visitor
        and call it's visitFile method with this File as parameter"""
        aFileObjectVisitor.visitFile(self)

