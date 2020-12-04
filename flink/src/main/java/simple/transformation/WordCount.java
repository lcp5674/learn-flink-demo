package simple.transformation;

/**
 * @Author:lcp
 * @CreateTime:2020-10-14 08:23:56
 * @Mark:
 **/
public class WordCount {
    public String word;
    public Integer count;

    public WordCount() {

    }

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }



    public static WordCount of(String word, Integer count) {
        return new WordCount(word, count);
    }


    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
