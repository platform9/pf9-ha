# This file is used as a lock sharing module between multiple files

import threading

VM_EVACUATION_LOCK=threading.Lock()
VM_EVACUATION_QUEUE=[]