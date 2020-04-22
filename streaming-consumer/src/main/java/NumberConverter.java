import java.util.HashMap;

public class NumberConverter {
    private static HashMap<String, Integer> map = InitMap();

    public static int ConvertToNumber(String numberString) {
        Integer num = tryParse(numberString);
        if (num != null)
        {
            return num;
        }

        String[] split = numberString.replace('-', ' ').split(" ");
        if (split.length > 2) {
            return 0;
        }

        if (split.length == 2) {
            int num1 = map.getOrDefault(split[0], 0);
            int num2 = map.getOrDefault(split[1], 0);

            return num1 + num2;
        }

        int number = map.getOrDefault(split[0], 0);

        return number;
    }

    private static Integer tryParse(String numberString) {
        Integer retVal;
        try {
            retVal = Integer.parseInt(numberString);
        } catch (NumberFormatException nfe) {
            retVal = null;
        }
        return retVal;
    }

    private static HashMap<String, Integer> InitMap() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put("four", 4);
        map.put("five", 5);
        map.put("six", 6);
        map.put("seven", 7);
        map.put("eight", 8);
        map.put("nine", 9);
        map.put("ten", 10);
        map.put("eleven", 11);
        map.put("twelve", 12);
        map.put("thirteen", 13);
        map.put("fourteen", 14);
        map.put("fifteen", 15);
        map.put("sixteen", 16);
        map.put("seventeen", 17);
        map.put("eighteen", 18);
        map.put("nineteen", 19);
        map.put("twenty", 20);
        map.put("thirty", 30);
        map.put("forty", 40);
        map.put("fifty", 50);
        map.put("sixty", 60);
        map.put("seventy", 70);
        map.put("eighty", 80);
        map.put("ninety", 90);

        return map;
    }
}
