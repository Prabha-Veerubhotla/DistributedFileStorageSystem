import FileService_pb2
import FileService_pb2_grpc
import grpc
import os

class RouteClient(FileService_pb2_grpc.FileserviceServicer):

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
        self.stub = FileService_pb2_grpc.FileserviceStub(self.channel)

    def generate_file_chunks(self, message):
        seq = 0
        with open(message.strip(), 'r') as f:
                while True:
                    chunk = f.read(4194000)
                    if not chunk:
                        break
                    seq = seq + 1
                    print("sending seq num: {} to server".format(seq))
                    yield FileService_pb2.FileData(username=self.name, filename=message, data=bytes(chunk,'utf-8'))

    def put(self, message):
        print("calling route client put")
        if self.stub.FileSearch(FileService_pb2.FileInfo(username = self.name, filename = message)).success:
            return "File: {} already present".format(message)
        file_iterator = self.generate_file_chunks(message)
        response = self.stub.UploadFile(file_iterator)
        if response.success:
            return "Successfully saved file: {}".format(message)
        else:
            return "Unable to save file: {}".format(message)

    
    def get(self, message):
        print("calling route client get")
        if not self.stub.FileSearch(FileService_pb2.FileInfo(username = self.name, filename = message)).success:
            return "File {} is not present".format(message) 

        fileinfo = FileService_pb2.FileInfo(username = self.name, filename = message)
        responses = self.stub.DownloadFile(fileinfo)
        outputfilename = "output-"+message.strip()
        mode = 'ab' if os.path.exists(outputfilename) else 'wb'
        with open(outputfilename, mode) as f:
            for response in responses:
               # print("writing seq num: {} into file".format(response.seqnum))
                f.write(response.data)
            f.close()
        return "File retrieved: {}".format("output-"+message)


    def update(self, message):
        print("calling route client update")
        if not self.stub.FileSearch(FileService_pb2.FileInfo(username = self.name, filename = message)).success:
            return "File: {} not present".format(message)

        file_iterator = self.generate_file_chunks(message)
        response = self.stub.UpdateFile(file_iterator)
        if response.success:
            return "Successfully updated file: {}".format(message)
        else:
            return "Unable to update file: {}".format(message)

    def search(self, message):
        print("calling route client search")
        fileinfo = FileService_pb2.FileInfo(username = self.name, filename = message)
        ack = self.stub.FileSearch(fileinfo)
        if ack.success: 
            return "{} is present".format(message)
        else:
            return "{} is not present".format(message)

    def delete(self, message):
        print("calling route client delete")
        if not self.stub.FileSearch(FileService_pb2.FileInfo(username = self.name, filename = message)).success:
            return "File: {} not present".format(message)
        fileinfo = FileService_pb2.FileInfo(username = self.name, filename = message)
        ack = self.stub.FileDelete(fileinfo)
        if ack.success:
            return "Successfully deleted {}".format(message)
        else:
            return "Unable to delete file {}".format(message)

    def list(self):
        print("calling route client list")
        userinfo = FileService_pb2.UserInfo(username = self.name)
        ack = self.stub.FileList(userinfo)
        if not ack:
            return "Unable to retrieve list of files saved"
        else:
            return ack









    



        




    

        



    







