/**
 * @description
 * @author: 王乾坤
 * @create: 2019-03-05 17:10:19
 **/

public class Test {
  public ListNode FindKthToTail(ListNode head, int k) {
    if (head == null) return head;
    if (k <= 0) return null;
    ListNode p = head, q = null;

    for (int i = 1; i < k; i++) {
      if (p.next != null) p = p.next;
      else return null;

    }
    q = head;
    while (p.next != null) {
      p = p.next;
      q = q.next;

    }
    return q;
  }
}