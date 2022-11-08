import 'dart:math';

var prefix = [
  "가냘픈",
  "가는",
  "가엾은",
  "가파른",
  "같은",
  "거센",
  "거친",
  "건조한",
  "검은",
  "게으른",
  "힘겨운",
  "힘찬",
];

var animals = ["사자", "호랑이", "알파카", "고릴라", "펭귄", "팬더", "코끼리", "다람쥐", "독수리", "곰"];

landumeNick() {
  Random random = Random();

  String randomNick =
      "${prefix[random.nextInt(prefix.length)]} ${animals[random.nextInt(animals.length)]}";

  return randomNick;
}
