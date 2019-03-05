import java.util.ArrayList;
import java.util.ListIterator;

public class Solution {
  ArrayList<Integer> arrayList = new ArrayList<Integer>();//建立一个新的列表

  public ArrayList<Integer> printListFromTailToHead(ListNode listNode) {
    if (listNode != null) {
      printListFromTailToHead(listNode.next);//指向下一个节点
      arrayList.add(listNode.val);//将当前节点的值存到列表中
    }
    return arrayList;
  }
  
  public static void main(String[] args){
    Solution solution = new Solution();
    ListNode listNode1 = new ListNode(1);
    ListNode listNode2 = new ListNode(2);
    ListNode listNode3 = new ListNode(3);
    listNode1.next = listNode2;
    listNode2.next = listNode3;
    ArrayList<Integer> integers = solution.printListFromTailToHead(listNode1);
    ListIterator<Integer> integerListIterator = integers.listIterator();
    while (integerListIterator.hasNext()){
      Integer next = integerListIterator.next();
      System.out.println(next);
    }
  }
}



