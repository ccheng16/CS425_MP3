package SDS;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

 public interface MapleJuice extends Remote{
	 public HashMap<String,List<Integer>> sayhello(List<String> t) throws RemoteException;
	 public Map<String,String> sayBye(HashMap<String,List<Integer>> h)throws RemoteException;
	 public HashMap<String, List<Integer>> RlMaple(List<String> linesList) throws RemoteException;
	 public HashMap<String, String> RlJuice(HashMap<String, List<Integer>> interMap) throws RemoteException;
	 public HashMap<String, List<Integer>> WcMaple(List<String> linesList) throws RemoteException;
	 public HashMap<String, String> WcJuice(HashMap<String, List<Integer>> interMap) throws RemoteException;
 }

