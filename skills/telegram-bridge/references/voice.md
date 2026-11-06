# Voice

Read when authoring a `telegram_voice` comment or handling a voice note that arrived without a transcript. The comment syntax itself is in `SKILL.md` under Voice.

## Writing for the ear

Author voice text for the ear, not the eye. The spoken text is yours to write; it
need not match the displayed reply:

- Restore proper spelling and diacritics even when the user typed without them.
- Set `lang` to the dominant language of the spoken text (for example `cs` for Czech) so a native voice reads it; leave `lang` unset for heavily mixed text.
- Expand abbreviations, numbers, URLs, and file paths into speakable words. Say "pi dot dev" rather than a raw URL.
- No emoji, no bullet syntax, no heading marks.
- Write speech as one flowing stream, the way people actually leave voice notes. Every period becomes a long synthetic pause, so prefer commas and connectors over full stops, and never use staccato runs of short sentences. One or two periods per note is plenty.
- Use natural spoken connectors of the target language to carry the flow, and contractions where the language has them.
- Keep one note under roughly forty-five seconds of speech; split longer content into more than one `telegram_voice` comment.
- If synthesis fails, the bridge falls back to a text reply on its own; never send the same content twice.

## Inbound voice recovery

A voice or audio attachment normally arrives with its
transcript in `[outputs]`. When the transcript is missing or empty (the
transcription handler was not loaded, or it failed), do not guess and do not
tell the user it cannot be read. Transcribe it yourself:

```bash
~/.pi/agent/skills/edge-tts/scripts/transcribe-gemini.sh --file <attachment-path>
```

This works on any audio file the bridge has downloaded, including ones from
before the handler existed. Only report failure if that command itself fails,
and then say exactly that the transcription service failed.
