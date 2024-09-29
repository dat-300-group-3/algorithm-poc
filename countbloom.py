import random
from functools import reduce


class cb:
   
    def __init__(self, ele, counters, k,removed, added):
        
        self.ele = ele
        self.counters = counters
        self.k = k
        self.removed = removed
        self.added = added
        self.c = 0

        self.counter = [0 for i in range(self.counters)]

    def gen(self):
        self.elements=random.sample(range(0,10000000000000),ele)
        
    def gen_rem(self):
        self.removed_elements=random.sample(range(0,10000000000000),self.removed)

    def gen_add(self):
        self.added_elements=random.sample(range(0,10000000000000),self.added)

    def hash(self):
        self.s = random.sample(range(0, 10000000000000), self.k)

    def encode(self):

        i,j=0,0
        while(i<len(self.elements)):
            while(j<len(self.s)):
                l=[self.elements[i],self.s[j]]
                res = reduce(lambda x, y: x ^ y, l)
                ind = res % len(self.counter)
                self.counter[ind] +=1
                j=j+1
            i=i+1
            j=0
        

    def removing(self):
        i,j=0,0
        while(i<len(self.removed_elements)):
            while(j<len(self.s)):
                l=[self.removed_elements[i],self.s[j]]
                res = reduce(lambda x, y: x ^ y, l)
                ind = res % len(self.counter)
                self.counter[ind] -=1
                j=j+1
            i=i+1
            j=0
        

    def adding(self):
        i,j=0,0
        while(i<len(self.added_elements)):
            while(j<len(self.s)):
                l=[self.added_elements[i],self.s[j]]
                res = reduce(lambda x, y: x ^ y, l)
                ind = res % len(self.counter)
                self.counter[ind] +=1
                j=j+1
            i=i+1
            j=0
        

    def lookup(self, element):
        i=0
        while(i<len(self.s)):

            l=[element,self.s[i]]
            res = reduce(lambda x, y: x ^ y, l)
            ind = res % len(self.counter)
            if self.counter[ind] !=0:
                pass
            else:
                return False
            i=i+1
        self.c += 1
        return True

if __name__ == "__main__":

    ele = int(input("no. of elements: "))
    removed =  int(input("no.of elements removed: "))
    added = int(input("no.elements added: "))
    counters =  int(input("no. of counters: "))
    k =  int(input("no. of hashes: "))
    i=0
    cbf = cb(ele, counters, k, removed, added)
    cbf.hash()
    cbf.gen()
    cbf.gen_rem()
    cbf.gen_add()
    cbf.encode()
    cbf.removing()
    cbf.adding()

    result = []
    while(i<len(cbf.elements)):
        result.append(cbf.lookup(cbf.elements[i]))
        i=i+1

    file = open('countingbloom.txt', 'w')
    print(str(cbf.c), file=file)




