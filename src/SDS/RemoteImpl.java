package SDS;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteImpl extends UnicastRemoteObject implements MapleJuice{
	protected RemoteImpl() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
	private static final long serialVersionUID = 4077329331699640331L;  
	@Override
	public HashMap<String, List<Integer>> sayhello(List<String> t) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Map<String,String> sayBye(HashMap<String,List<Integer>> h) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	 public HashMap<String, List<Integer>> RlMaple(List<String> linesList) throws RemoteException {	   	   	 
		   HashMap<String, List<Integer>> linkMap = new HashMap<String, List<Integer>>();	   
		   for (String line: linesList) {
			   String nodes[] = line.split("\\s+");
			   String source = nodes[0];
			   String target = nodes[1];
		   	   if (!linkMap.containsKey(target)) {
				   List<Integer> initialList = new ArrayList<Integer>();
				   initialList.add(Integer.parseInt(source));
				   linkMap.put(target, initialList);
			   }
			   else {				   
				   ((ArrayList<Integer>) linkMap.get(target)).add(Integer.parseInt(source));
			   }		   		   
		   }
		   return linkMap;
	   }
	@Override
	public HashMap<String, String> RlJuice(HashMap<String, List<Integer>> interMap) throws RemoteException {
		   HashMap<String, String> resultMap = new HashMap<String, String>();	   
		   for (String key: interMap.keySet()) {
			   StringBuilder sourceList = new StringBuilder();
			   for (Integer source: interMap.get(key)) {
			        sourceList.append(source);
			        sourceList.append(" ");
			   }
			   resultMap.put(key, sourceList.toString());
		   }
		   return resultMap;
	}
	
	public HashMap<String, List<Integer>> WcMaple(List<String> linesList) {
		   HashMap<String, List<Integer>> wordMap = new HashMap<String, List<Integer>>();	   
		   for (String line: linesList) {
			   String words[] = line.split("\\s+");
			   for (String word: words) {
			   		if (word.startsWith(" ")) {
						continue;
			   		}
				   if (!wordMap.containsKey(word)) {
					   List<Integer> initialList = new ArrayList<Integer>();
					   initialList.add(1);
					   wordMap.put(word, initialList);
				   }
				   else {
					   ((ArrayList<Integer>) wordMap.get(word)).add(1);
				   }
			   }
		   }
		   return wordMap;
	   }
	
	public HashMap<String, String> WcJuice(HashMap<String, List<Integer>> interMap) {	   	   	 
		   HashMap<String, String> resultMap = new HashMap<String, String>();	   
		   for (String key: interMap.keySet()) {
			   int sum = interMap.get(key).stream().mapToInt(Integer::intValue).sum();
			   resultMap.put(key, Integer.toString(sum));
		   }
		   return resultMap;
	   }
}