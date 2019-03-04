import java.util.Stack;

/**
 * @description
 * @author: 王乾坤
 * @create: 2019-02-27 10:02:41
 **/
public class Test {
  public static void main(String[] args){
    Stack<Character> stack = new Stack<Character>();
    String str = "abcd";
    char[] chars = str.toCharArray();
    for (char aChar : chars) {
      stack.push(aChar);
    }
    
  }
}
