import FileService_pb2
import FileService_pb2_grpc
import grpc
import os

class RouteClient():

    def __init__(self,ip, port):
        self.ip = ip
        self.port = port
        self.name = "client"
        
    def setClientName(self, clientname):
        self.name = clientname
        print("Setting client name as: {}".format(self.name))
        
    def startClient(self):
        print("Starting route client ...")
        self.channel = grpc.insecure_channel(self.ip+self.port)
        self.stub = FileService_pb2_grpc.FileServiceStub(self.channel)

    def generate_file_chunks(self, message):
        filedata = FileService_pb2.FileData
        filedata.filename = message
        filedata.username = self.name
        seq = 0
        with open(message, 'rb') as f:
                while True:
                    chunk = f.read(4194000)
                    if not chunk:
                        break
                    filedata.content = chunk
                    seq = seq + 1
                    filedata.seqnum = seq
                    yield filedata

    def put(self, message):
        print("calling route client put")
        if self.stub.searchFile(message).success:
            return "File: {} already present".format(message)
        file_iterator = self.generate_file_chunks(message)
        response = self.stub.UploadFile(file_iterator)
        if response.success:
            return "Successfully saved file: {}".format(message)
        else:
            return "Unable to save file: {}".format(message)

    
    def get(self, message):
        print("calling route client get")
        if not self.stub.searchFile(message).success:
            return "File {} is not present".format(message) 

        fileinfo = FileService_pb2.FileInfo
        fileinfo.username = self.name
        fileinfo.filename = message
        responses = self.stub.DownloadFile(fileinfo)
        mode = 'a' if os.path.exists("output-"+message) else 'w'
        with open(message, mode) as f:
            for response in responses:
                print("writing seq num: {} into file".format(response.seqnum))
                f.write(response.content)
            f.close()
        return "File retrieved: {}".format("output-"+message)


    def update(self, message):
        print("calling route client update")
        if not self.stub.searchFile(message).success:
            return "File: {} not present".format(message)

        file_iterator = self.generate_file_chunks(message)
        response = self.stub.updateFile(file_iterator)
        if response.success:
            return "Successfully updated file: {}".format(message)
        else:
            return "Unable to update file: {}".format(message)

    def search(self, message):
        print("calling route client search")
        fileinfo = FileService_pb2.FileInfo
        fileinfo.username = self.name
        fileinfo.filename = message
        ack = self.stub.searchFile(message)
        if ack.success: 
            return "{} is present".format(message)
        else:
            return "{} is not present".format(message)

    def delete(self, message):
        print("calling route client delete")
        if not self.stub.searchFile(message).success:
            return "File: {} not present".format(message)
        fileinfo = FileService_pb2.FileInfo
        fileinfo.username = self.name
        fileinfo.filename = message
        ack = self.stub.deleteFile(fileinfo)
        if ack.success:
            return "Successfully deleted {}".format(message)
        else:
            return "Unable to delete file {}".format(message)

    def list(self):
        print("calling route client list")
        userinfo = FileService_pb2.UserInfo
        userinfo.username = self.name
        ack = self.stub.deleteFile(userinfo)
        if not ack:
            return "Unable to retrieve list of files saved"
        else:
            return ack



    





    



        




    

        



    







