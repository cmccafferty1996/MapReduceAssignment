import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapReduce {
	
	private static String threads = "";
	private static int numThreads = -1;
	//private static String files = "";
	//private static int numFiles = -1;
	
	public static void main(String[] args) throws ExecutionException {
		// the problem:
	        
        // from here (INPUT)
	        
        // "file1.txt" => "foo foo bar cat dog dog"
        // "file2.txt" => "foo house cat cat dog"
        // "file3.txt" => "foo foo foo bird"

        // we want to go to here (OUTPUT)
        
        // "foo" => { "file1.txt" => 2, "file3.txt" => 3, "file2.txt" => 1 }
        // "bar" => { "file1.txt" => 1 }
        // "cat" => { "file2.txt" => 2, "file1.txt" => 1 }
        // "dog" => { "file2.txt" => 1, "file1.txt" => 2 }
        // "house" => { "file2.txt" => 1 }
        // "bird" => { "file3.txt" => 1 }
        
        // in plain English we want to
        
        // Given a set of files with contents
        // we want to index them by word 
        // so I can return all files that contain a given word
        // together with the number of occurrences of that word
        // without any sorting
		
		// Scammer to read inputs
		Scanner in = new Scanner(System.in);
		
		// Ask for the number of threads
		System.out.println("Enter the number of Threads to use:");
		while (numThreads < 0){
			try {
				threads = in.nextLine();
				numThreads = Integer.parseInt(threads);
				if (numThreads < 0){
					System.out.println("Please enter a positive whole number!");
				}
			} catch (NumberFormatException ex){
				System.out.println("Please enter a positive whole number!");
			}
		}
		
		// Make the Thread pool
		ExecutorService executor = 
        		Executors.newFixedThreadPool(numThreads);//creating a pool of threads 
        
        ////////////
        // INPUT:
        ///////////
		
        File inFile = new File("/MapReduceAssignment/res/Oxford_English_Dictionary.txt");
        String fileContents = null;
		try {
			fileContents = 
					new String(Files.readAllBytes(Paths.get("C:/Users/cmcca/workspace/MapReduceAssignment/res/Oxford_English_Dictionary.txt")));
		} catch (IOException e1) {
			System.out.println("Error getting file contents");
			e1.printStackTrace();
		}
		
		File inFile2 = new File("/MapReduceAssignment/res/Paradise_Lost.txt");
        String fileContents2 = null;
		try {
			fileContents2 = 
					new String(Files.readAllBytes(Paths.get("C:/Users/cmcca/git/MapReduceAssignment/res/Paradise_Lost.txt")));
		} catch (IOException e1) {
			System.out.println("Error getting file contents");
			e1.printStackTrace();
		}
        
        Map<String, String> input = new HashMap<String, String>();
        input.put(inFile.getName(), fileContents);
        input.put(inFile2.getName(), fileContents2);
        //input.put("file3.txt", "foo foo foo bird");
        
        // APPROACH #1: Brute force
        {
                Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                long start = System.currentTimeMillis();
                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while(inputIter.hasNext()) {
                        Map.Entry<String, String> entry = inputIter.next();
                        String file = entry.getKey();
                        String contents = entry.getValue();
                        
                        String[] words = contents.trim().split("\\s+");
                        
                        for(String word : words) {
                                
                                Map<String, Integer> files = output.get(word);
                                if (files == null) {
                                        files = new HashMap<String, Integer>();
                                        output.put(word, files);
                                }
                                
                                Integer occurrences = files.remove(file);
                                if (occurrences == null) {
                                        files.put(file, 1);
                                } else {
                                        files.put(file, occurrences.intValue() + 1);
                                        }
                                }
                        }
                
                // show me:
                System.out.println("Approach 1 - Brute Force");
                long end = System.currentTimeMillis();
                long time = end - start;
                System.out.println("Time taken: "+time/1000.00+" secs");
                System.out.println();
                //System.out.println(output);
        }

        
        // APPROACH #2: MapReduce
        {
                Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                long start = System.currentTimeMillis();
                // MAP:
                
                List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                
                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while(inputIter.hasNext()) {
                        Map.Entry<String, String> entry = inputIter.next();
                        String file = entry.getKey();
                        String contents = entry.getValue();
                        
                        map(file, contents, mappedItems);
                }
                
                // GROUP:
                
                Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                
                Iterator<MappedItem> mappedIter = mappedItems.iterator();
                while(mappedIter.hasNext()) {
                        MappedItem item = mappedIter.next();
                        String word = item.getWord();
                        String file = item.getFile();
                        List<String> list = groupedItems.get(word);
                        if (list == null) {
                                list = new LinkedList<String>();
                                groupedItems.put(word, list);
                        }
                        list.add(file);
                }
                
                // REDUCE:
                
                Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                while(groupedIter.hasNext()) {
                        Map.Entry<String, List<String>> entry = groupedIter.next();
                        String word = entry.getKey();
                        List<String> list = entry.getValue();
                        
                        reduce(word, list, output);
                }
                System.out.println("Approach 2 - MapReduce");
                long end = System.currentTimeMillis();
                long time = end - start;
                System.out.println("Time taken: "+time/1000.00+" secs");
                System.out.println();
                //System.out.println(output);
        }
        
     // APPROACH #3: Distributed MapReduce
        {
                final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                
                // MAP:
                long start = System.currentTimeMillis();
                final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                
                final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                        @Override
        public synchronized void mapDone(String file, List<MappedItem> results) {
            mappedItems.addAll(results);
        }
                };
                
                List<Thread> mapCluster = new ArrayList<Thread>(input.size());
                
                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while(inputIter.hasNext()) {
                        Map.Entry<String, String> entry = inputIter.next();
                        final String file = entry.getKey();
                        final String contents = entry.getValue();
                        
                        Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                        	map(file, contents, mapCallback);
                        }
                        });
                        mapCluster.add(t);
                        t.start();
                    }
                
                // wait for mapping phase to be over:
                for(Thread t : mapCluster) {
                        try {
                                t.join();
                        } catch(InterruptedException e) {
                                throw new RuntimeException(e);
                        }
                }
                
                // GROUP:
                
                Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                
                Iterator<MappedItem> mappedIter = mappedItems.iterator();
                while(mappedIter.hasNext()) {
                        MappedItem item = mappedIter.next();
                        String word = item.getWord();
                        String file = item.getFile();
                        List<String> list = groupedItems.get(word);
                        if (list == null) {
                                list = new LinkedList<String>();
                                groupedItems.put(word, list);
                        }
                        list.add(file);
                }
                
                // REDUCE:
                
                final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                        @Override
        public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
        }
                };
                
                List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
                
                Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                while(groupedIter.hasNext()) {
                        Map.Entry<String, List<String>> entry = groupedIter.next();
                        final String word = entry.getKey();
                        final List<String> list = entry.getValue();
                        
                        Thread t = new Thread(new Runnable() {
                                @Override
            public void run() {
                                        reduce(word, list, reduceCallback);
                                }
                        });
                        reduceCluster.add(t);
                        t.start();
                }
                
                // wait for reducing phase to be over:
                for(Thread t : reduceCluster) {
                        try {
                                t.join();
                        } catch(InterruptedException e) {
                                throw new RuntimeException(e);
                        }
                }
                long end = System.currentTimeMillis();
                long time = end-start;
                System.out.println("Approach 3");
                System.out.println("Time taken: "+time/1000.00+" secs");
                //System.out.println(output);
                System.out.println();
        }
	        
	        
        // APPROACH #3: Modified Distributed MapReduce (Thread pool)
        {
                final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                
                // MAP:
                long start = System.currentTimeMillis();
                final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                
                final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                        @Override
        public synchronized void mapDone(String file, List<MappedItem> results) {
            mappedItems.addAll(results);
        }
                };
                
                List<Future> mapFutures = new ArrayList<Future>(input.size()); 
                
                Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                while(inputIter.hasNext()) {
                        Map.Entry<String, String> entry = inputIter.next();
                        final String file = entry.getKey();
                        final String contents = entry.getValue();
                        
                        Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                        	map(file, contents, mapCallback);
                        	}
                        });
                        mapFutures.add(executor.submit(t));
                        //mapCluster.add(t);
                        //t.start();
                }
                
                
                // wait for mapping phase to be over:
                for(Future current : mapFutures) {
                	try {
                		// get method blocks until thread is finished
                		current.get();
                	} catch(InterruptedException | ExecutionException e) {
                		e.printStackTrace();
                	}
                }
                
                // GROUP:
                
                Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                
                Iterator<MappedItem> mappedIter = mappedItems.iterator();
                while(mappedIter.hasNext()) {
                        MappedItem item = mappedIter.next();
                        String word = item.getWord();
                        String file = item.getFile();
                        List<String> list = groupedItems.get(word);
                        if (list == null) {
                                list = new LinkedList<String>();
                                groupedItems.put(word, list);
                        }
                        list.add(file);
                }
                
                // REDUCE:
                
                final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                        @Override
        public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
        }
                };
                
                List<Future> reduceFutures = new ArrayList<Future>(groupedItems.size());
                
                Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                while(groupedIter.hasNext()) {
                        Map.Entry<String, List<String>> entry = groupedIter.next();
                        final String word = entry.getKey();
                        final List<String> list = entry.getValue();
                        
                        Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                        	reduce(word, list, reduceCallback);
                        	}
                        });
                        reduceFutures.add(executor.submit(t));
//                      reduceCluster.add(t);
//                      t.start();
                }
                
                
                // wait for reducing phase to be over:
                for(Future current : reduceFutures) {
                	try {
                		// get method blocks until thread is finished
                		current.get();
                	} catch(InterruptedException | ExecutionException e) {
                		e.printStackTrace();
                	}
                }
                long end = System.currentTimeMillis();
                long time = end-start;
                System.out.println("Approach 3 modified - uses thread pools");
                System.out.println("Time taken: "+time/1000.00+" secs");
                System.out.println();
                //System.out.println(output);
        }
        
        // APPROACH #4: Distributed MapReduce and parallel group phase
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
            
            // MAP:
            long start = System.currentTimeMillis();
            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
            
            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                    @Override
    public synchronized void mapDone(String file, List<MappedItem> results) {
        mappedItems.addAll(results);
    }
            };
            
            List<Future> mapFutures = new ArrayList<Future>(input.size()); 
            
            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                    Map.Entry<String, String> entry = inputIter.next();
                    final String file = entry.getKey();
                    final String contents = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                    	map(file, contents, mapCallback);
                    	}
                    });
                    mapFutures.add(executor.submit(t));
                    //mapCluster.add(t);
                    //t.start();
            }
            
            
            // wait for mapping phase to be over:
            for(Future current : mapFutures) {
                try {
                	// get method blocks until thread is finished
                	current.get();
                } catch(InterruptedException e) {
                	throw new RuntimeException(e);
                }
            }
            
            // GROUP:
            
            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
            List<Future> groupFutures = new ArrayList<Future>(); 
            
            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                    MappedItem item = mappedIter.next();
                    String word = item.getWord();
                    String file = item.getFile();
                    Thread t = new Thread(new Runnable() {
                    	@Override
                        public void run() {
                    		List<String> list = groupedItems.get(word);
                        	if (list == null) {
                        		list = new LinkedList<String>();
                                groupedItems.put(word, list);
                        	}
                        list.add(file);
                        }
                    });
                    groupFutures.add(executor.submit(t));
            }
            
            // wait for grouping phase to be over:
            for(Future current : groupFutures) {
            	try {
            		// get method blocks until thread is finished
            		current.get();
            	} catch(InterruptedException | ExecutionException e) {
            		e.printStackTrace();
            	}
            }
            
            // REDUCE:
            
            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                    @Override
    public synchronized void reduceDone(String k, Map<String, Integer> v) {
            output.put(k, v);
    }
            };
            
            List<Future> reduceFutures = new ArrayList<Future>(groupedItems.size());
            
            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                    Map.Entry<String, List<String>> entry = groupedIter.next();
                    final String word = entry.getKey();
                    final List<String> list = entry.getValue();
                    
                    Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                    	reduce(word, list, reduceCallback);
                    	}
                    });
                    reduceFutures.add(executor.submit(t));
//                  reduceCluster.add(t);
//                  t.start();
            }
            
            // wait for reducing phase to be over:
            for(Future current : reduceFutures) {
            	try {
            		// get method blocks until thread is finished
            		current.get();
            	} catch(InterruptedException | ExecutionException e) {
            		e.printStackTrace();
            	} catch (NullPointerException npe){
            		// catch but don't do anything
            		// program continues to run fine
            	}
            }
            
            long end = System.currentTimeMillis();
            long time = end-start;
            System.out.println("Approach 4 Parallel Group phase");
            System.out.println("Time taken: "+time/1000.00+" secs");
            //System.out.println(output);
            System.out.println();
        }
        in.close();
	}
	
	public static void map(String file, String contents, List<MappedItem> mappedItems) {
	        String[] words = contents.trim().split("\\s+");
	        for(String word: words) {
	                mappedItems.add(new MappedItem(word, file));
	        }
	}
	
	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
	        Map<String, Integer> reducedList = new HashMap<String, Integer>();
	        for(String file: list) {
	                Integer occurrences = reducedList.get(file);
	                if (occurrences == null) {
	                        reducedList.put(file, 1);
	                } else {
	                        reducedList.put(file, occurrences.intValue() + 1);
	                }
	        }
	        output.put(word, reducedList);
	}
	
	public static interface MapCallback<E, V> {
	        
	        public void mapDone(E key, List<V> values);
	}
	
	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
	        String[] words = contents.trim().split("\\s+");
	        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
	        for(String word: words) {
	                results.add(new MappedItem(word, file));
	        }
	        callback.mapDone(file, results);
	}
	
	public static interface ReduceCallback<E, K, V> {
	        
	        public void reduceDone(E e, Map<K,V> results);
	}
	
	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
		
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                    reducedList.put(file, 1);
            } else {
            	reducedList.put(file, occurrences.intValue() + 1);
            	}
            }
        callback.reduceDone(word, reducedList);
        }
	
	private static class MappedItem { 
	        
        private final String word;
        private final String file;
        
        public MappedItem(String word, String file) {
        	this.word = word;
            this.file = file;
            }

        public String getWord() {
            return word;
        	}

        public String getFile() {
        	return file;
        	}
        
        @Override
        public String toString() {
                return "[\"" + word + "\",\"" + file + "\"]";
                }
        }
	} 
