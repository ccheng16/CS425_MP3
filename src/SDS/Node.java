package SDS; 

 import org.apache.commons.collections4.ListUtils; 
 import org.jgroups.*;
 import org.jgroups.JChannel;
 import org.jgroups.Message;
 import org.jgroups.Address;
 import org.jgroups.ReceiverAdapter;
 import org.jgroups.View;
 import org.jgroups.util.UUID;
 import org.jgroups.util.Util;
 import org.jgroups.conf.ClassConfigurator;
 
 import java.nio.charset.StandardCharsets;
 import java.net.*;
 import java.rmi.Naming;
 import java.rmi.registry.LocateRegistry;
 import java.io.*;
 import java.util.*;
 import java.util.stream.Collectors;
 import java.util.concurrent.ConcurrentHashMap;
 import java.util.function.Supplier;
 
 
 public class Node extends ReceiverAdapter {
     JChannel channel;
     String user_name = System.getProperty("user.name", "n/a");
     final List<String> entryList = new LinkedList<>();
     final List<String> commandList = new LinkedList<>();
     protected Map<String,OutputStream> files = new ConcurrentHashMap<>();
     protected static final short ID = 3500;
     public final static ArrayList<String> ipList=getIP();
 	
	 private static ArrayList<String> getIP(){
	   	ArrayList<String> rst = new ArrayList<String>();
	   	rst.add("172.22.149.58");
	   	rst.add("172.22.149.59");
	   	rst.add("172.22.149.60");
	   	rst.add("172.22.149.61");
	   	rst.add("172.22.149.62");
	   	rst.add("172.22.149.63");
	   	rst.add("172.22.149.64");
	   	rst.add("172.22.149.65");
	   	rst.add("172.22.149.66");
	   	rst.add("172.22.149.67");
	   	return rst;
	 }
	 
     public static void runServer(){
     	MapleJuice mr;
 		try {
 			mr = new RemoteImpl();
 	        LocateRegistry.createRegistry(2002);
 	    	//LocateRegistry.getRegistry(2002);	    	
 	        Naming.rebind("rmi://localhost:2002/mr", mr);
 	        System.out.println("Server is Ready");
 		} catch (Exception e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}
     }
     
     public void viewAccepted(View newView) {
         System.out.println("Current view: " + channel.getView());
         List<Address> addrs = newView.getMembers();



         if (addrs.indexOf(channel.getAddress()) == 0) { 
         // If this channel is the coordinator
         try {

              if (!commandList.isEmpty()) {
                String lostCommand = commandList.get(commandList.size()-1);
                System.out.println("The executing command " + lostCommand + " is Lost");

                InputStream inStream = new ByteArrayInputStream(lostCommand.getBytes(StandardCharsets.UTF_8));

                // StringInputStream s = new StringInputStream(lostCommand);
                System.setIn(inStream);
              }

             List<String> members = addrs.stream().map(Object::toString).collect(Collectors.toList());
             System.out.println("Current members:");
             for (String member: members) {
                  System.out.println(member);
             }
             ArrayList<String> lostFiles = new ArrayList<String>();
             LinkedList<String> tempList = new LinkedList<String>(entryList);
             for (String entry: tempList) { //sdfsFilename@addrStr@logicalAddr
                  String[] parts = entry.split("@");
                  if (!members.contains(parts[2])) {
                     System.out.println(parts[0] + " at " + parts[2] + " failed");
                     lostFiles.add(parts[0]);
                     entryList.remove(entry);
                     Message entryMsg = new Message(null,"unset:" + entry);
                     channel.send(entryMsg);
                 }
                 }
             System.out.println("Lost file list is:");
             for (String lostFile: lostFiles) {
                 System.out.println(lostFile);
             }
             for (String lostFile: lostFiles) {
                 System.out.println("Lost file: " + lostFile);
                 HashMap<String, Boolean> targets = new HashMap<String, Boolean>();
                 // (addrStr,t/f)
                 if (entryList.size() == 0) {
                     System.out.println("The entryList is empty.");
                 }
                 
                 for (String entry: entryList)
                 { //sdfsFilename@addrStr@logicalAddr
                     
                     System.out.println(entry);
                     String[] parts = entry.split("@");
                     if (!entry.startsWith(lostFile + "@")) {                        
                         targets.put(parts[1], false);
                     }
                     
                 }
                 for (String entry: entryList)
                 { //sdfsFilename@addrStr@logicalAddr
                     String[] parts = entry.split("@");
                     if (entry.startsWith(lostFile + "@")) {                        
                         targets.put(parts[1], true);
                        // System.out.println(parts[1] + " has " + lostFile);
 
                     }
 
                 }
                 // Re-replicate lost files
                 String sender = "";
                 String receiver = "";
 
                 for (String a:targets.keySet()) {
                     if(targets.get(a) == false) {
                         receiver = a;
                         System.out.println("receiver is " + a);
                     }
                     if(targets.get(a) == true) {
                         sender = a;
                         System.out.println("sender is " + a);
                     }
                     if((sender != "") && (receiver != ""))
                     {
                         break;
                     }
                 }
 
                 String repMessage = "rep:" + lostFile + ":" + receiver;
                 System.out.println((UUID.fromString(sender)).toString() + " re-replicating " + lostFile + " to " 
                     + (UUID.fromString(receiver)).toString());
                 Message repMsg = new Message(UUID.fromString(sender),repMessage);
                 channel.send(repMsg);
             }
         }
         catch (Exception e) { 
         e.printStackTrace();           
         }
         }
     }
     
     @SuppressWarnings("resource")
     private void start() throws Exception {
         ClassConfigurator.add((short)3500, FileHeader.class);
         channel = new JChannel();//.name("SDFS");
         channel.setReceiver(this);
         channel.connect("SDFS");
         channel.getState(null, 10000);
         try {
  			  runServer();
  		} catch (Exception e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  		}
         eventLoop();
         channel.close();
     }
 
     public void getState(OutputStream output) throws Exception {
         synchronized(entryList) {
             Util.objectToStream(entryList, new DataOutputStream(output));
         }
     }
 
     public void setState(InputStream input) throws Exception {
         List<String> list = Util.objectFromStream(new DataInputStream(input));
         synchronized(entryList) {
             entryList.clear();
             entryList.addAll(list);
         }
         System.out.println("Recovered entryList (size: " + list.size() + "):");
         list.forEach(System.out::println);
     }
 
     public void receive(Message msg) {
         FileHeader hdr = (FileHeader) msg.getHeader(ID);              
         if (hdr == null) {
             Address sender = msg.getSrc();
             String line = msg.getObject();            
             if (line.startsWith("set")) {
                 String[] parts = line.split(":");
                 synchronized(entryList) {
                     if (!entryList.contains(parts[1])) {
                         entryList.add(parts[1]);    
                     }                    
                 }
             }
             if (line.startsWith("cset")) {
                 String[] parts = line.split(":");
                 synchronized(commandList) {
                     if (!commandList.contains(parts[1])) {
                         commandList.add(parts[1]);    
                     }                    
                 }
             }
             if (line.startsWith("unset")) {
                 String[] parts = line.split(":");
                 synchronized(entryList) {
                     if (entryList.contains(parts[1])) {
                         entryList.remove(parts[1]);
                     }                    
                 }
             }
             if (line.startsWith("rep")) {
                 String[] parts = line.split(":");
                 //rep:lostFile:receiver;
                 try {
                     sendFile(UUID.fromString(parts[2]), false, "./sdfs/" + parts[1], parts[1]);
                     System.out.println("File " + parts[1] + " re-replicated to " + 
                         UUID.fromString(parts[2]).toString());
                 }
                 catch (Exception e) { 
                 e.printStackTrace();                   
                 }
             }
             if (line.startsWith("get")) { //UNCHECKED
                 // get sdfsfilename localfilename (fetches to local dir)
                 String[] parts = line.split(" ");
                 String sdfsFilename = parts[1];
                 String localFilename = parts[2];
                 try {
                     sendFile(sender, true, localFilename, sdfsFilename);
                     System.out.println("Sending file " + sdfsFilename + " to " + sender );
                 }
                 catch (Exception e) {   
                 e.printStackTrace();                 
                 }
             }
             if (line.startsWith("delete")) {
                 String[] parts = line.split(" ");
                 String sdfsFilename = parts[1];
                 try {
                     File victim = new File("./sdfs/" + sdfsFilename);
                     if (victim.delete()) {
                         System.out.println("File " + sdfsFilename + " deleted at " + channel.getAddress());
                         // Tell other members the file has been deleted here
                         String entry = "unset:" + sdfsFilename + "@" + 
                         ((UUID)channel.getAddress()).toStringLong() + "@" + channel.getAddress();
                         Message entryMsg = new Message(null,entry);
                         channel.send(entryMsg);
                     }
                     else {
                         System.out.println("Deleting failed: file not found.");
                     }
                 }
                 catch (Exception e) { 
                 e.printStackTrace();                   
                 }
             }            
         }
         else {            
             byte[] buf = msg.getRawBuffer();
             String file;
             if (hdr.isLocal) {
                 file = "./local/" + hdr.localFilename;
             }
             else {
                 file = "./sdfs/" + hdr.sdfsFilename;
             }
             OutputStream out = files.get(file);
             try {
                 if(out == null) {
                     File tmp = new File(file);                    
                     out = new FileOutputStream(file);
                     files.put(file, out);                    
                 }
                 if(hdr.eof) {
                     Util.close(files.remove(file));
                     System.out.println("Received file: " + file);
                     if (!hdr.isLocal) {
                         String entry = "set:" + hdr.sdfsFilename + "@" + 
                         ((UUID)channel.getAddress()).toStringLong() + "@" + channel.getAddress();
                         Message entryMsg = new Message(null,entry);
                         channel.send(entryMsg);
                     }
                 }
                 else {
                     out.write(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                 }
             }
             catch(Throwable t) {
                 System.err.println(t);
             }
         }
     }
 

	private void eventLoop() {
         BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
         while (true) {
             try {                
                 System.out.flush();
                 String line = in.readLine().toLowerCase();
                 if(line.startsWith("exit")) {
                     break;
                 }
                 if(line.startsWith("put")) { //TODO
                     // Instruction format: put localfilename sdfsfilename (from local dir)
                     // Parsing instruction
                     String[] parts = line.split(" ");
                     String localFilename = parts[1];
                     String sdfsFilename = parts[2];
                     putFile(localFilename,sdfsFilename);
                    
                 }                
                 if(line.startsWith("delete")) {
                     // delete sdfsfilename
                     String[] parts = line.split(" ");
                     String sdfsFilename = parts[1];

                     deleteFile(sdfsFilename);

                     // for (String entry: entryList) {
                     //     if (entry.startsWith(sdfsFilename + "@")) {
                     //         String[] str = entry.split("@"); //sdfsFilename@addrStr@logicalAddr
                     //         System.out.println("SDFS file " + sdfsFilename + " found at " + str[2]);
                     //         Message msg = new Message(UUID.fromString(str[1]), line);
                     //         channel.send(msg);
                     //     }
                     // }
                 }
                 if(line.startsWith("get")) {
                     // get sdfsfilename localfilename (fetches to local dir)
                     String[] parts = line.split(" ");
                     String sdfsFilename = parts[1];
                     String localFilename = parts[2];
                     getFile(sdfsFilename,localFilename);
                 }
                 
                 if(line.startsWith("store")) {
                     // list the set of file names that are replicated (stored) on SDFS at that process. 
                     // This should not include files stored on the local file system.
                     String localAddr = channel.getAddress().toString();
                     for (String entry: entryList) {
                         if (entry.endsWith(localAddr)){
                             String[] str = entry.split("@"); //sdfsFilename@addrStr@logicalAddr
                             System.out.println("Local store has file: " + str[0]);
                         }
                     }
                 }
                 if(line.startsWith("ls")) {
                     // ls sdfsfilename: list all VM addresses where this file is currently replicated 
                     // (If you are splitting files into blocks, just set the block size to be large enough 
                     // that each file is one block)
                     String[] parts = line.split(" ");
                     String sdfsFilename = parts[1];
                     for (String entry: entryList) {
                         if (entry.startsWith(sdfsFilename + "@")) {
                             String[] str = entry.split("@"); //sdfsFilename@addrStr@logicalAddr
                             System.out.println("SDFS file " + sdfsFilename + " has replicate at " + str[2]);
                         }
                     }
                 }
 
                 if(line.startsWith("list")) {
                     View currView = channel.getView();
                     System.out.println("Current view: " + currView);
                     System.out.println("View ID: " + currView.getViewId());
                     System.out.println("Local address: " + channel.getAddress());
                 }
 
                 if(line.startsWith("show")) {                    
                     System.out.println("entryList is shown as: ");
                     for (String entry: entryList) {
                         System.out.println(entry);
                     }
                     System.out.println("commandList is shown as: ");
                     for (String command: commandList) {
                         System.out.println(command);
                     }
                 }  
                 
                  
                  if(line.startsWith("maple")){
                      System.out.println("Got command: " + line);
                      String commandEntry = "cset:" + line;
                      Message commandMsg = new Message(null,commandEntry);
                      channel.send(commandMsg);

                	  String temp[]=line.split("\\s");
                	  String function = temp[1];
                      int mapleNum = Integer.parseInt(temp[2]);
                      getFile(temp[4], "totalInput");

                      File f= new File("./local/totalInput");
                      while(!f.isFile()){
                           // Thread.sleep(100);
                      }

                      BufferedReader reader= new BufferedReader(new FileReader("./local/totalInput"));
                      String content="";
                      List<String> rst = new ArrayList<String>();
                      while((content = reader.readLine()) != null){
                    	   rst.add(content);
                      }
                      reader.close();
                      System.out.println(rst);
                      List<List<String>> output = ListUtils.partition(rst, rst.size()/mapleNum+1);
                      ArrayList<ArrayList<String>> output1 = new ArrayList<ArrayList<String>>();
                      System.out.println("split in to"+output.size());
                      for(int i=0;i<output.size();i++){
                        output1.add(new ArrayList<String>(output.get(i)));
                      }
                      ArrayList<MapleJuice> objects = new ArrayList<MapleJuice>();
                      ArrayList<Map<String,List<Integer>>> mapleRst=new ArrayList<Map<String,List<Integer>>>();
                      for(int i=0;i<mapleNum;i++){
                    	  MapleJuice mr1=(MapleJuice) Naming.lookup("rmi://"+ipList.get(i)+":2002/mr");
                    	  objects.add(mr1);
                    	 // mapleRst.add(objects.get(i).sayhello(output.get(i)));
                   	   switch(temp[1])
                   	     {
                   	     	case "wordcount.exe":
                   	            mapleRst.add(objects.get(i).WcMaple(output1.get(i)));
                   	            break;//call remote function
                   	       case "reverselink.exe":
                   	       	 mapleRst.add(objects.get(i).RlMaple(output1.get(i)));
                   	     		 break;
                   	       default:
                   	          System.out.println("This function cannot be found!");
                   	         break;
                   	     }                   	                        	   
                      }
                      for(Map<String,List<Integer>> map: mapleRst){
                        for(String s: map.keySet()){
                            System.out.println("key: "+s+" "+map.get(s));
                        }
                      }
                      System.out.println("remote function get invoked!");
                      Map<String, List<Integer>> shuffulRst=new HashMap <String, List<Integer>>();
                     
                     //shufful-phase
                      for(Map<String,List<Integer>> map: mapleRst){
                    	  for(String s: map.keySet())
                    		  if(!shuffulRst.containsKey(s)){
                    			  shuffulRst.put(s, map.get(s));
                    		  }
                    		  else{
                    			  List<Integer> current=shuffulRst.get(s);
                    			  current.addAll(map.get(s));
                    			  shuffulRst.put(s, current);
                    		  }
                      }
                    
                    for(String s: shuffulRst.keySet()){
                      PrintWriter fout = new PrintWriter("sdfs_intermediate_"+s);
                      for(int i: shuffulRst.get(s)){
                    	  fout.println(i);
        				  fout.flush();
                      }
                      fout.close();
                      putFile("sdfs_intermediate_"+s,"sdfs_intermediate_"+s);
                    }          			                      
                  }
                  
                  if(line.startsWith("juice")){

                      String commandEntry = "cset:" + line;
                      Message commandMsg = new Message(null,commandEntry);
                      channel.send(commandMsg);

                	  String temp[]=line.split("\\s");
                	  String function=temp[1];
                	  String delete=temp[5];
                      int juiceNum=Integer.parseInt(temp[2]);
                      //
                      ArrayList<String> interFile=new ArrayList<String> ();
                      for (String entry: this.entryList) {
                          if (entry.startsWith("sdfs_intermediate_")) {
                        	  String [] name=entry.split("@");
                        	  interFile.add(name[0]);
                          }
                      }
                      HashMap<String,List<Integer>> shuffleData=new  HashMap<String,List<Integer>>();
                      for(String file: interFile){
                    	  String [] keys=file.split("_");
                          int kSize=keys.length-1;
                    	  getFile(file,keys[kSize]);
                          File f= new File("./local/"+keys[kSize]);
                          while(!f.isFile()){
                               // Thread.sleep(100);
                          }
                    	  BufferedReader reader= new BufferedReader(new FileReader("./local/"+keys[kSize]));
                    	  String content="";
                          List<Integer> rst=new ArrayList<Integer>();
                          while((content=reader.readLine())!=null){
                        	   rst.add(Integer.parseInt(content));
                          }
                          shuffleData.put(keys[kSize], rst);
                          reader.close();
                          if(delete.equals("1"))
                        	  deleteFile(file);
                              //System.out.println("delete file");//need correction.
                        	  
                      }
                      //
                     ArrayList<HashMap<String,List<Integer>>> output= new ArrayList<HashMap<String,List<Integer>>> ();
                     List<String> names=new ArrayList<String>();
                     names.addAll(shuffleData.keySet());  
                     List<List<String>> keySplit = ListUtils.partition(names,names.size()/juiceNum+1);

                      ArrayList<MapleJuice> objects=new ArrayList<MapleJuice>();//
                      ArrayList<Map<String,String>> juiceRst=new ArrayList<Map<String,String>>();//
                      for(int i=0;i<juiceNum;i++){
                    	  MapleJuice mr1=(MapleJuice) Naming.lookup("rmi://"+ipList.get(i)+":2002/mr");
                    	  objects.add(mr1);
                    	  HashMap<String,List<Integer>> trans=new HashMap<String,List<Integer>> ();
                    	  for(String s: keySplit.get(i)){
                    		  trans.put(s, shuffleData.get(s));
                    	  }
                    	   //juiceRst.add(objects.get(i).sayBye(trans));//call remote function
                    	   switch(temp[1])
                    	     {
                    	     	case "wordcount.exe":
                    	            juiceRst.add(objects.get(i).WcJuice(trans));
                    	            break;//call remote function
                    	       case "reverselink.exe":
                    	       	    juiceRst.add(objects.get(i).RlJuice(trans));
                    	     		 break;
                    	      default:
                    	         System.out.println("This function cannot be found!");
                    	         break;
                    	     }                    	                        	    
                      }
                      
                      TreeMap<String, String> finalOutput=new TreeMap<String,String>();
                      for(Map<String,String> m:juiceRst){
                    	  for(String s: m.keySet()){
                    		  finalOutput.put(s, m.get(s));
                    	  }
                      }
                      long time=System.currentTimeMillis()%10000;
                      String lag=Long.toString(time);
                      PrintWriter fout = new PrintWriter("sdfs_dest_"+lag);
                      for(String s: finalOutput.keySet()){                                                  
                        	  fout.println(s+":"+finalOutput.get(s));
            				  fout.flush();
                      }
                          fout.close();
                      
                      putFile("sdfs_dest_"+lag,"sdfs_dest_"+lag);                                 			                     
                  }
                  
             }
             catch(Exception e) {
                e.printStackTrace();
             }
         }
     }
 
     protected void sendFile(Address dst, boolean isLocal, String localFilename, String sdfsFilename) throws Exception {
         FileInputStream in = new FileInputStream(isLocal?"./sdfs/" + sdfsFilename:localFilename);
         // FileInputStream in = new FileInputStream(localFilename);
         // System.out.println("File " + localFilename + " replicated to " + dst + " as " + sdfsFilename);
         try {
             // int count = 0;
             while (true) {
                 byte[] buf = new byte[8096];
                 int bytes = in.read(buf);
                 if(bytes == -1)
                     break;
                 sendMessage(dst, buf, 0, bytes, false, isLocal, localFilename, sdfsFilename);
                 buf = null; // Clear the buffer
                 // count += 8;
                 // System.out.println(count + "KB transferred.");
             }
         }
         catch(Exception e) {
             e.printStackTrace();
         }
         finally {
             sendMessage(dst, null, 0, 0, true, isLocal, localFilename, sdfsFilename);
             // System.out.println("Completed!");
         }
     }
 
     protected void sendMessage(Address dst, byte[] buf, int offset, int length, boolean eof, boolean isLocal,
                                  String localFilename, String sdfsFilename) throws Exception 
     {
         Message msg = new Message(dst, buf, offset, length).putHeader(ID, 
             new FileHeader(localFilename, sdfsFilename, eof, isLocal));
         // set this if the sender doesn't want to receive the file
         // msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
         channel.send(msg);
     }
 
     protected static class FileHeader extends Header {
         protected String localFilename;
         protected String sdfsFilename;
         protected boolean eof;
         protected boolean isLocal;
         public FileHeader() {} // for de-serialization
         public FileHeader(String localFilename, String sdfsFilename, boolean eof, boolean isLocal) {
             this.localFilename = localFilename;
             this.sdfsFilename = sdfsFilename;
             this.eof = eof;
             this.isLocal = isLocal;
         }
         public int size() {
             return Util.size(localFilename) + Util.size(sdfsFilename) + 2*Global.BYTE_SIZE;
         }
         public void writeTo(DataOutput out) throws Exception {
             Util.writeObject(localFilename, out);
             Util.writeObject(sdfsFilename, out);
             out.writeBoolean(eof);
             out.writeBoolean(isLocal);
         }
         public void readFrom(DataInput in) throws Exception {
             localFilename = (String)Util.readObject(in);
             sdfsFilename = (String)Util.readObject(in);
             eof = in.readBoolean();
             isLocal = in.readBoolean();
         }
         public short getMagicId() {
             return 3500;
         }
         public Supplier<? extends Header> create() {
             return FileHeader::new;
         }
     }    

     protected void putFile(String localFilename, String sdfsFilename) throws Exception {
         View currView = this.channel.getView();
         List<Address> targets = new LinkedList<>();
         if (currView.size() <= 3) {
             targets = currView.getMembers();
         }
         else {
             targets.add(this.channel.getAddress());
             int index = currView.getMembers().indexOf(this.channel.getAddress());
             Random targetOne = new Random();
             int oneIndex = (targetOne.nextInt(currView.size()-2) + index + 1) % currView.size();
             int twoIndex = (oneIndex + 1) % currView.size();
             targets.add(currView.getMembers().get(oneIndex));
             targets.add(currView.getMembers().get(twoIndex));
         }
         for (Address target: targets) {
             UUID utarget = (UUID) target;
             String addrStr = utarget.toStringLong();                        
             // Message msg = new Message(null, line);
             // this.channel.send(msg);
             System.out.println("Replicating file to address: " + target);
             sendFile(UUID.fromString(addrStr), false, localFilename, sdfsFilename);
         }        
     }
    
     protected void getFile(String sdfsFilename, String localFilename) throws Exception {
         boolean found = false;
         for (String entry: this.entryList) {
             if (entry.startsWith(sdfsFilename + "@")) {
                 found = true;
                 String[] str = entry.split("@"); //sdfsFilename@addrStr@logicalAddr
                 System.out.println("SDFS file " + sdfsFilename + " found at " + str[2]);
                 Message msg = new Message(UUID.fromString(str[1]), "get " + sdfsFilename + " " + localFilename);
                 channel.send(msg);
                 break;
             }
         }
         if(!found) System.out.println("File " + sdfsFilename + " not found in SDFS.");
     }

     protected void deleteFile(String sdfsFilename) throws Exception {
        for (String entry: entryList) {
             if (entry.startsWith(sdfsFilename + "@")) {
                 String[] str = entry.split("@"); //sdfsFilename@addrStr@logicalAddr
                 System.out.println("SDFS file " + sdfsFilename + " found at " + str[2]);
                 Message msg = new Message(UUID.fromString(str[1]), "delete " + sdfsFilename);
                 this.channel.send(msg);
             }
         }
     }

     public static void main(String[] args) throws Exception {
         Node newNode = new Node();
         newNode.start();
     }
 }