import sys
from RouteClient import RouteClient

class MessageClient():
    def __init__(self, ip):
        self.ip = ip
        # self.port = "2345"
        self.port = "9000"
        print("Intializing Message Client config..")
        print("Connecting to server on: {} :{} ".format(self.ip, self.port))
        self.name = "client"
        self.rc = RouteClient(self.ip, self.port)
        self.setClientName()

    def setClientName(self):
        print("Enter your name in order to join: ")
        message = sys.stdin.readline()
        self.name = message.strip()
        self.rc.setClientName(message)
        self.startClientSession()
    
    def startClientSession(self):
        self.rc.startClient()
        self.availableCli()
    
    def availableCli(self):
        print("Welcome  {} ".format(self.name))
        print("Commands")
        print("-----------------------------------------------")
        print("help - show this menu")
        print("whoami - list my settings")
        self.handleHelp()
        self.handleChoice()

    def handleHelp(self):
        print("put - send a file (default)")
        print("get - retrieve a stored  file")
        print("update - update a stored file")
        print("list - list all stored  files")
        print("delete - delete a stored file")
        print("search - know if a file is present")
        print("exit - end session")
        print("")

    def handleOtherChoices(self, choice):
        if 'put' in choice:
            print("Enter file name to save: ")
            message = sys.stdin.readline()
            putstatus = self.rc.put(message)
            print(putstatus)        
        elif 'get' in choice:
            print("Enter file name to retrieve: ")
            message = sys.stdin.readline()
            getstatus = self.rc.get(message)
            print(getstatus)
        elif 'update' in choice:
            print("Enter file name to update: ")
            message = sys.stdin.readline()
            updatestatus = self.rc.update(message)
            print(updatestatus)
        elif 'list' in choice:
            print(self.rc.list())
        elif 'delete' in choice:
            print("Enter file name to delete: ")
            message = sys.stdin.readline()
            deletestatus = self.rc.delete(message)
            print(deletestatus)
        elif 'search' in choice:
            print("Enter file name to search: ")
            message = sys.stdin.readline()
            searchstatus = self.rc.search(message)
            print(searchstatus)
        else:
            # same as put
            message = sys.stdin.readline()
            putstatus = self.rc.put(message)
            print(putstatus)


    def handleChoice(self):
        while(True):
            choice = sys.stdin.readline()
            if 'help' in choice:
                self.handleHelp()
            elif 'whoami' in choice:
                print("You are {}".format(self.name))
            elif 'exit' in choice:
                break
            else:
                self.handleOtherChoices(choice)
            

        self.stopClient()

    def stopClient(self):
        sys.exit(0)   

...

def run(ip):
        print("Starting client cli...")
        mc = MessageClient(ip)

if __name__ == '__main__':
    run(sys.argv[1])
